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
package com.google.enterprise.cloudsearch.sdk.indexing;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.enterprise.cloudsearch.sdk.ConnectorContext;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.StartupException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.Parser;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl.InheritanceType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Common default ACL object that can be used for an entire data repository.
 *
 * <p>The common ACL can be designated 'entire domain accessible' or can define specific readers
 * that will be used for the entire repository. It is considered a configuration error to specify
 * both 'entire domain accessible' while also specifying a specific set of readers, etc.
 *
 * <p>Optional configuration parameters:
 *
 * <ul>
 *   <li>{@code defaultAcl.mode} - Specifies the default ACL mode ({@link DefaultAclMode}).
 *   <li>{@code defaultAcl.public} - Specifies ({@code true}/{@code false}), that the common ACL
 *       used for the entire repository has "public" access.
 *   <li>{@code defaultAcl.readers.users} - Specifies the common ACL readers
 *       in a comma delimited list (only used when "public" is {@code false}).
 *   <li>{@code defaultAcl.readers.groups} - Specifies the common ACL group readers in a comma
 *       delimited list (only used when "public" is {@code false}).
 *   <li>{@code defaultAcl.denied.users} - Specifies the common ACL denied readers in a comma
 *       delimited list (only used when "public" is {@code false}).
 *   <li>{@code defaultAcl.denied.groups} - Specifies the common ACL denied group readers in a
 *       comma delimited list (only used when "public" is {@code false}).
 *   <li>{@code defaultAcl.name} - Specifies the virtual container name of
 *       the default ACL item.  Set this parameter especially if multiple
 *       connectors may be used on the same data source in parallel to prevent
 *       them from interfering with each other. This item's queue is always
 *       designated as {@code DEFAULT_ACL_VIRTUAL_CONTAINER_QUEUE}. Default: {@code
 *       DEFAULT_ACL_VIRTUAL_CONTAINER}
 * </ul>
 *
 * <p>Sample usage from the connector initialization
 * ({@link com.google.enterprise.cloudsearch.sdk.Connector#init(ConnectorContext)}):
 *
 * <pre>{@code
 * IndexingService myIndexingServiceInstance = context.getIndexingService();
 *
 * // one time creation of the default ACL based on configuration parameters (see above)
 * DefaultAcl defaultAcl = DefautAcl.fromConfiguration(myIndexingServiceInstance);
 * }</pre>
 *
 * <p>And then later during iteration through the repository data:
 *
 * <pre>{@code
 * // while looping through the repository data
 * ...
 * Item item = ... // create the Item for uploading
 * defaultAcl.applyToIfEnabled(item); // will update the item's ACL depending on the mode
 * // upload the item
 * ...
 * }</pre>
 */
public class DefaultAcl {

  public static final String DEFAULT_ACL_MODE = "defaultAcl.mode";
  public static final String DEFAULT_ACL_PUBLIC = "defaultAcl.public";
  public static final String DEFAULT_ACL_READERS_USERS = "defaultAcl.readers.users";
  public static final String DEFAULT_ACL_READERS_GROUPS = "defaultAcl.readers.groups";
  public static final String DEFAULT_ACL_DENIED_USERS = "defaultAcl.denied.users";
  public static final String DEFAULT_ACL_DENIED_GROUPS = "defaultAcl.denied.groups";
  public static final String DEFAULT_ACL_NAME = "defaultAcl.name";

  public static final String DEFAULT_ACL_NAME_DEFAULT = "DEFAULT_ACL_VIRTUAL_CONTAINER";
  public static final String DEFAULT_ACL_QUEUE = "DEFAULT_ACL_VIRTUAL_CONTAINER_QUEUE";

  /**
   * Configuration parser for {@link DefaultAclMode}.
   */
  public static final Parser<DefaultAclMode> DEFAULT_ACL_MODE_PARSER =
      value -> {
        try {
          return DefaultAclMode.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
          throw new InvalidConfigurationException(
              "Invalid default ACL mode config value: " + value, e);
        }
      };

  /**
   * Mode setting used to set a default ACL on an {@link Item}.
   */
  public enum DefaultAclMode {
    /**
     * Does not use any default ACL values.
     */
    NONE("none"),
    /**
     * Specifies that default ACL values are used only if no ACLs are already defined.
     */
    FALLBACK("fallback"),
    /**
     * Specifies that default ACL values are appended to the existing ACL definitions.
     */
    APPEND("append"),
    /**
     * Specifies that the default ACL values replace any existing ACL definitions.
     */
    OVERRIDE("override");

    private String value;

    DefaultAclMode(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    /**
     * Determines if default ACLs are being used.
     *
     * @return {@code true} only if the mode is not {@link DefaultAclMode#NONE}
     */
    public boolean isEnabled() {
      return this != NONE;
    }
  }

  private final Acl commonAcl;
  private final DefaultAclMode aclMode;
  private final String defaultAclName;

  /**
   * Creates a default ACL object and uploads it to Cloud Search.
   *
   * <p>If default ACLs are used by the connector, create a virtual container item for the default
   * ACL with the defined permissions. Upload this "ACL parent" item for use in {@link
   * #applyToIfEnabled(Item)} for assigning an <em>inherit from</em> item.
   *
   * @param builder the builder object containing all default ACL settings
   */
  private DefaultAcl(Builder builder) {
    aclMode = builder.aclMode;
    defaultAclName = builder.defaultAclName;
    if (!aclMode.isEnabled()) {
      commonAcl = null;
    } else {
      if (builder.isPublic) {
        commonAcl = new Acl.Builder()
            .setReaders(Collections.singleton(Acl.getCustomerPrincipal()))
            .build();
      } else {
        List<Principal> readers = new ArrayList<Principal>();
        readers.addAll(builder.readerUsers);
        readers.addAll(builder.readerGroups);
        List<Principal> deniedReaders = new ArrayList<Principal>();
        deniedReaders.addAll(builder.deniedReaderUsers);
        deniedReaders.addAll(builder.deniedReaderGroups);
        commonAcl = new Acl.Builder()
            .setReaders(readers)
            .setDeniedReaders(deniedReaders)
            .build();
      }
      Item aclParent = new IndexingItemBuilder(defaultAclName)
          .setItemType(ItemType.VIRTUAL_CONTAINER_ITEM)
          .setAcl(commonAcl)
          .setQueue(DEFAULT_ACL_QUEUE)
          .build();
      try {
        Operation operation =
            builder.indexingService.indexItem(aclParent, RequestMode.SYNCHRONOUS).get();
        // Done indicates operation is completed. In SYNCHRONOUS mode returned operation should
        // always be marked as Done. Done represents state of the operation but not actual result.
        if (!operation.getDone()) {
          throw new StartupException(String.format("Unable to upload default ACL. %s", operation));
        }
        // Check if there are any errors while creating container item.
        if (operation.getError() != null) {
          throw new StartupException(String.format("Unable to upload default ACL. %s", operation));
        }
      } catch (IOException e) {
        throw new StartupException("Unable to upload default ACL.", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new StartupException(
            "Interrupted while creating virtual container item for Default ACL.", e);
      } catch (ExecutionException e) {
        throw new StartupException("Unable to upload default ACL.", e.getCause());
      }
    }
  }

  /**
   * Creates an instance of a {@link DefaultAcl} from the {@link Configuration}.
   *
   * @param indexingService connector's indexing service instance for uploading default ACL to
   * Cloud Search
   * @return an instance of a {@link DefaultAcl}
   */
  public static DefaultAcl fromConfiguration(IndexingService indexingService) {
    checkState(Configuration.isInitialized(), "Configuration not initialized");
    DefaultAclMode aclMode =
        Configuration.getValue(DEFAULT_ACL_MODE, DefaultAclMode.NONE, DEFAULT_ACL_MODE_PARSER)
            .get();
    boolean defaultAclPublic = Configuration.getBoolean(DEFAULT_ACL_PUBLIC, false).get();
    List<Principal> readerUsers =
        Configuration.getMultiValue(
                DEFAULT_ACL_READERS_USERS, Collections.emptyList(), Acl.USER_PARSER)
            .get();
    List<Principal> readerGroups =
        Configuration.getMultiValue(
                DEFAULT_ACL_READERS_GROUPS, Collections.emptyList(), Acl.GROUP_PARSER)
            .get();
    List<Principal> deniedReaderUsers =
        Configuration.getMultiValue(
                DEFAULT_ACL_DENIED_USERS, Collections.emptyList(), Acl.USER_PARSER)
            .get();
    List<Principal> deniedReaderGroups =
        Configuration.getMultiValue(
                DEFAULT_ACL_DENIED_GROUPS, Collections.emptyList(), Acl.GROUP_PARSER)
            .get();
    String defaultAclName =
        Configuration.getString(DEFAULT_ACL_NAME, DEFAULT_ACL_NAME_DEFAULT)
            .get();
    return new DefaultAcl.Builder()
        .setMode(aclMode)
        .setIsPublic(defaultAclPublic)
        .setReaderUsers(readerUsers)
        .setReaderGroups(readerGroups)
        .setDeniedReaderUsers(deniedReaderUsers)
        .setDeniedReaderGroups(deniedReaderGroups)
        .setIndexingService(indexingService)
        .setDefaultAclName(defaultAclName)
        .build();
  }

  /**
   * Applies a common default ACL to the passed {@link Item}.
   *
   * <p>The mode defines how default ACLs are applied:
   * <ul>
   *   <li>{@link DefaultAclMode#NONE} - Default ACL is not being used.
   *   <li>{@link DefaultAclMode#FALLBACK} - Default ACL is only used if none are already present.
   *   <li>{@link DefaultAclMode#APPEND} - Default ACL is added to the existing ACL.
   *   <li>{@link DefaultAclMode#OVERRIDE} - Default ACL replaces any existing ACL.
   * </ul>
   *
   * @param item the object to apply the default ACL
   * @return {@code true} if the common ACL was applied to this {@link Item}
   */
  public boolean applyToIfEnabled(Item item) {
    if (!aclMode.isEnabled()) { // do nothing if default ACL not being used
      return false;
    }
    checkNotNull(item, "Item cannot be null.");

    if (isAclEmpty(item)) {
      // In every mode use case, if the item has no ACL information (aside from an owner), just set
      // the "inheritFrom" to the ACL parent.
      applyInheritedAcl(item);
      return true;
    } else {
      switch (aclMode) {
        case FALLBACK: // only set if no current ACL being used
          return false;

        case APPEND: // add to existing ACL
          List<Principal> appendedReaders = getCopyOrEmptyList(item.getAcl().getReaders());
          appendedReaders.addAll(commonAcl.getReaders());
          List<Principal> appendedDeniedReaders =
              getCopyOrEmptyList(item.getAcl().getDeniedReaders());
          appendedDeniedReaders.addAll(commonAcl.getDeniedReaders());
          Acl.Builder appendedAcl =
              new Acl.Builder()
                  .setReaders(appendedReaders)
                  .setDeniedReaders(appendedDeniedReaders)
                  .setInheritFrom(item.getAcl().getInheritAclFrom())
                  .setOwners(getCopyOrEmptyList(item.getAcl().getOwners()));
          if (!Strings.isNullOrEmpty(item.getAcl().getAclInheritanceType())) {
            appendedAcl.setInheritanceType(
                InheritanceType.valueOf(item.getAcl().getAclInheritanceType()));
          }
          appendedAcl.build().applyTo(item);
          return true;

        case OVERRIDE: // replace existing ACL
          applyInheritedAcl(item);
          return true;

        default: // should never get this far
          throw new IllegalStateException(
              "Attempting to apply a default ACL with mode: " + aclMode.toString());
      }
    }
  }

  /**
   * Checks whether an item's ACL is empty.
   *
   * <p>The ACL is considered empty if it is {@code null} or if it only has an owner defined.
   *
   * @param item the item to check
   * @return true if the ACL is considered empty
   */
  @VisibleForTesting
  static boolean isAclEmpty(Item item) {
    ItemAcl itemAcl = item.getAcl();
    return (itemAcl == null)
        || (isNullOrEmpty(itemAcl.getReaders())
            && isNullOrEmpty(itemAcl.getDeniedReaders())
            && (itemAcl.getInheritAclFrom() == null));
  }

  private static List<Principal> getCopyOrEmptyList(Collection<Principal> input) {
    // Create a copy here. It's possible that input is immutable collection.
    return input == null ? new ArrayList<>() : new ArrayList<>(input);
  }

  private static boolean isNullOrEmpty(Collection<?> collection) {
    return collection == null || collection.isEmpty();
  }

  /**
   * Applies a new inherited ACL preserving only the owners if present.
   *
   * @param item the item to use for applying a new inherited ACL
   */
  private void applyInheritedAcl(Item item) {
    Acl.Builder aclBuilder = new Acl.Builder()
        .setInheritFrom(defaultAclName)
        .setInheritanceType(InheritanceType.PARENT_OVERRIDE);
    // preserve any owners
    ItemAcl itemAcl = item.getAcl();
    if ((itemAcl != null) && !isNullOrEmpty(itemAcl.getOwners())) {
      aclBuilder.setOwners(itemAcl.getOwners());
    }
    Acl acl = aclBuilder.build();
    acl.applyTo(item);
  }

  /**
   *  Returns DefaultAclMode.
   */
  public DefaultAclMode getDefaultAclMode() {
    return aclMode;
  }

  /**
   * Builder object for constructing a {@link DefaultAcl} object.
   */
  public static final class Builder {

    private DefaultAclMode aclMode = DefaultAclMode.NONE;
    private boolean isPublic = true;
    private List<Principal> readerUsers = Collections.emptyList();
    private List<Principal> readerGroups = Collections.emptyList();
    private List<Principal> deniedReaderUsers = Collections.emptyList();
    private List<Principal> deniedReaderGroups = Collections.emptyList();
    private IndexingService indexingService;
    private String defaultAclName = DEFAULT_ACL_NAME_DEFAULT;

    public Builder() {
    }

    public Builder setMode(DefaultAclMode aclMode) {
      this.aclMode = aclMode;
      return this;
    }

    public Builder setIsPublic(boolean isPublic) {
      this.isPublic = isPublic;
      return this;
    }

    public Builder setReaderUsers(List<Principal> readerUsers) {
      this.readerUsers = readerUsers;
      return this;
    }

    public Builder setReaderGroups(List<Principal> readerGroups) {
      this.readerGroups = readerGroups;
      return this;
    }

    public Builder setDeniedReaderUsers(List<Principal> deniedReaderUsers) {
      this.deniedReaderUsers = deniedReaderUsers;
      return this;
    }

    public Builder setDeniedReaderGroups(List<Principal> deniedReaderGroups) {
      this.deniedReaderGroups = deniedReaderGroups;
      return this;
    }

    public Builder setIndexingService(IndexingService indexingService) {
      this.indexingService = indexingService;
      return this;
    }

    public Builder setDefaultAclName(String defaultAclName) {
      this.defaultAclName = defaultAclName;
      return this;
    }

    public DefaultAcl build() {
      checkNotNull(readerUsers, "Default ACL readers cannot be null.");
      checkNotNull(readerGroups, "Default ACL reader groups cannot be null.");
      checkNotNull(deniedReaderUsers, "Default ACL denied readers cannot be null.");
      checkNotNull(deniedReaderGroups, "Default ACL denied reader groups cannot be null.");
      checkNotNull(indexingService, "Default ACL IndexingService cannot be null.");
      checkNotNull(defaultAclName, "Default ACL name cannot be null.");
      if (!aclMode.isEnabled() || isPublic) {
        checkArgument(
            readerUsers.isEmpty(),
            "can not specify reader users if default acl is public or not enabled");
        checkArgument(
            readerGroups.isEmpty(),
            "can not specify reader groups if default acl is public or not enabled");
        checkArgument(
            deniedReaderUsers.isEmpty(),
            "can not specify denied reader users if default acl is public or not enabled");
        checkArgument(
            deniedReaderGroups.isEmpty(),
            "can not specify denied reader groups if default acl is public or not enabled");
      }
      if (aclMode.isEnabled() && !isPublic) {
        checkArgument(
            !readerUsers.isEmpty()
                || !readerGroups.isEmpty()
                || !deniedReaderUsers.isEmpty()
                || !deniedReaderGroups.isEmpty(),
            "no principal specified for non public default ACL");
      }
      return new DefaultAcl(this);
    }
  }
}
