/*
 * Copyright © 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.enterprise.cloudsearch.sdk.identity;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.admin.directory.model.User;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/** Represents third-party user identity and corresponding Google identity mapping. */
public class IdentityUser extends IdentityPrincipal<IdentityUser> {
  private static final Logger logger = Logger.getLogger(IdentityUser.class.getName());
  private final String schema;
  private final String attribute;
  private final String googleIdentity;

  private IdentityUser(Builder builder) {
    super(checkNotNullOrEmpty(builder.userIdentity, "user identity can not be null or empty"));
    this.googleIdentity =
        checkNotNullOrEmpty(builder.googleIdentity, "google identity can not be null or empty");
    this.schema = checkNotNullOrEmpty(builder.schema, "schema can not be null or empty");
    this.attribute = checkNotNullOrEmpty(builder.attribute, "attribute can not be null or empty");
  }

  /**
   * Gets custom schema name under which identity will be mapped for user.
   *
   * @return custom schema name under which identity will be mapped for user.
   */
  public String getSchema() {
    return schema;
  }

  /**
   * Gets custom schema attribute name which will be populated to map third-party user identity.
   *
   * @return custom schema attribute name which will be populated to map third-party user identity.
   */
  public String getAttribute() {
    return attribute;
  }

  /**
   * Gets Google identity for user.
   *
   * @return Google identity for user.
   */
  public String getGoogleIdentity() {
    return googleIdentity;
  }

  /**
   * Get kind for {@link IdentityPrincipal}. This is always {@link IdentityPrincipal.Kind#USER} for
   * {@link IdentityUser}.
   */
  @Override
  public Kind getKind() {
    return Kind.USER;
  }

  /** Syncs {@link IdentityUser} with Google Admin SDK API using {@code service} */
  @Override
  public ListenableFuture<IdentityUser> sync(
      @Nullable IdentityUser previouslySynced, IdentityService service) throws IOException {
    if (this.equals(previouslySynced)) {
      return Futures.immediateFuture(this);
    }
    logger.log(Level.FINE, "Syncing user {0}", this);
    ListenableFuture<User> updateUserMapping =
        service.updateUserMapping(googleIdentity, schema, attribute, Optional.of(identity));
    return Futures.transform(
        updateUserMapping,
        (@Nullable User input) -> {
          checkState(input != null, "user can not be null");
          checkArgument(
              isSameUser(googleIdentity, input),
              "unexpected user object. expected %s got %s",
              googleIdentity,
              input);
          return this;
        },
        getExecutor());
  }

  /**
   * Unmaps {@link IdentityUser}, by clearing out {@link IdentityUser#getAttribute}, using Google
   * Admin SDK API.
   */
  @Override
  public ListenableFuture<Boolean> unmap(IdentityService service) throws IOException {
    logger.log(Level.FINE, "Unmapping user {0}", this);
    ListenableFuture<User> updateUserMapping =
        service.updateUserMapping(googleIdentity, schema, attribute, Optional.empty());
    return Futures.transform(
        updateUserMapping,
        new Function<User, Boolean>() {
          @Override
          @Nullable
          public Boolean apply(@Nullable User input) {
            checkNotNull(input, "updated user can not be null");
            checkArgument(
                isSameUser(googleIdentity, input),
                "unexpected user object. expected %s got %s",
                googleIdentity,
                input);
            return true;
          }
        },
        getExecutor());
  }

  @Override
  public int hashCode() {
    return Objects.hash(identity, googleIdentity, schema, attribute);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (!(obj instanceof IdentityUser)) {
      return false;
    }
    IdentityUser other = (IdentityUser) obj;
    return Objects.equals(identity, other.identity)
        && Objects.equals(googleIdentity, other.googleIdentity)
        && Objects.equals(schema, other.schema)
        && Objects.equals(attribute, other.attribute);
  }

  @Override
  public String toString() {
    return "IdentityUser [schema="
        + schema
        + ", attribute="
        + attribute
        + ", googleIdentity="
        + googleIdentity
        + ", identity="
        + identity
        + "]";
  }

  private Executor getExecutor() {
    return MoreExecutors.directExecutor();
  }

  private static boolean isSameUser(String googleIdentity, User user) {
    if (googleIdentity.equalsIgnoreCase(user.getPrimaryEmail())) {
      return true;
    }
    if (user.getAliases() == null) {
      return false;
    }
    return user.getAliases().stream().anyMatch(googleIdentity::equalsIgnoreCase);
  }

  /** Builder for {@link IdentityUser} */
  public static class Builder {
    private String userIdentity;
    private String googleIdentity;
    private String schema;
    private String attribute;

    /** Sets external user identity to be mapped. */
    public Builder setUserIdentity(String userIdentity) {
      this.userIdentity = userIdentity;
      return this;
    }

    /** Sets Google identity for user to map external identity. */
    public Builder setGoogleIdentity(String googleIdentity) {
      this.googleIdentity = googleIdentity;
      return this;
    }

    /** Sets custom schema name to be updated for identity mapping. */
    public Builder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    /** Sets custom schema attribute name to be populated for identity mapping. */
    public Builder setAttribute(String attribute) {
      this.attribute = attribute;
      return this;
    }

    /** Builds an instance of {@link IdentityUser} */
    public IdentityUser build() {
      return new IdentityUser(this);
    }
  }
}
