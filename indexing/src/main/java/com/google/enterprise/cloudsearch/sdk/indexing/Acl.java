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
import static com.google.common.base.Suppliers.memoize;

import com.google.api.services.cloudsearch.v1.model.GSuitePrincipal;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.enterprise.cloudsearch.sdk.ExternalGroups;
import com.google.enterprise.cloudsearch.sdk.GroupIdEncoder;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Represents all aspects of access permissions for an uploaded document.
 *
 * <p>The Access Control List (ACL) contains a list of both users and groups that have either read
 * access or denied access to an {@link Item}. Additionally, the access can be dependent on an
 * "inherit from" parent ACL corresponding to the {@link InheritanceType} value. A parent can also
 * have multiple ACLs associated with it as defined by its fragments set in {@link
 * Builder#setInheritFrom(String, String)}.
 *
 * <p>Instances are immutable.
 */
public class Acl {
  /** Prefix for identity source ID */
  public static final String IDENTITY_SOURCES_PREFIX = "identitysources";

  /** Format to construct identity source ID */
  public static final String IDENTITY_RESOURCE_NAME_FORMAT = "identitysources/%s";

  /** Format to construct external user principal name */
  public static final String USER_RESOURCE_NAME_FORMAT =
      IDENTITY_RESOURCE_NAME_FORMAT + "/users/%s";

  /** Format to construct external group principal name */
  public static final String GROUP_NAME_FORMAT = IDENTITY_RESOURCE_NAME_FORMAT + "/groups/%s";

  /**
   * {@link Configuration.Parser} to parse string value as external user or Google user principal
   */
  public static final UserPrincipalParser USER_PARSER = new UserPrincipalParser();

  /**
   * {@link Configuration.Parser} to parse string value as external group or Google group principal
   */
  public static final GroupPrincipalParser GROUP_PARSER = new GroupPrincipalParser();

  /** Configuration value prefix to indicate Google principal */
  public static final String GOOGLE_ACCOUNT_PREFIX = "google:";

  @VisibleForTesting static final String GOOGLE_USER_TYPE = "GSUITE_USER_EMAIL";
  static final String GOOGLE_GROUP_TYPE = "GSUITE_GROUP_EMAIL";
  static final String ENTIRE_DOMAIN_TYPE = "GSUITE_DOMAIN";
  static final String USER_TYPE = "USER";
  static final String GROUP_TYPE = "GROUP";
  static final String FRAGMENT_ID_FORMAT = "%s#%s";

  private final ImmutableSet<Principal> readers;
  private final ImmutableSet<Principal> deniedReaders;
  private final ImmutableSet<Principal> owners;
  private final String inheritFrom;
  private final String inheritFromFragment;
  private final InheritanceType inheritType;

  private Acl(Builder builder) {
    this.readers = builder.readers;
    this.deniedReaders = builder.deniedReaders;
    this.owners = builder.owners;
    this.inheritFrom = builder.inheritFrom;
    this.inheritFromFragment = builder.inheritFromFragment;
    this.inheritType = builder.inheritType;
  }

  /** Returns the allowed readers. */
  public Set<Principal> getReaders() {
    return getClonedCopy(readers, ImmutableSet.toImmutableSet());
  }

  /** Returns the denied readers. */
  public Set<Principal> getDeniedReaders() {
    return getClonedCopy(deniedReaders, ImmutableSet.toImmutableSet());
  }

  /** Returns the owners. */
  public Set<Principal> getOwners() {
    return getClonedCopy(owners, ImmutableSet.toImmutableSet());
  }

  /**
   * Returns the "inherit from" parent name.
   */
  public String getInheritFrom() {
    return inheritFrom;
  }

  /**
   * Returns the "inherit from" fragment.
   *
   * @see Builder#setInheritFrom(String, String)
   */
  public String getInheritFromFragment() {
    return inheritFromFragment;
  }

  /**
   * Returns the {@link InheritanceType}.
   */
  public InheritanceType getInheritanceType() {
    return inheritType;
  }

  /** Mutable ACL for creating instances of {@link Acl}. */
  public static class Builder {
    private ImmutableSet<Principal> readers = ImmutableSet.of();
    private ImmutableSet<Principal> deniedReaders = ImmutableSet.of();
    private ImmutableSet<Principal> owners = ImmutableSet.of();
    private String inheritFrom;
    private String inheritFromFragment;
    private InheritanceType inheritType;

    /**
     * Creates new empty builder.
     *
     * <p>All sets are empty, inheritFrom is {@code null}, and inheritType is
     * {@link InheritanceType#CHILD_OVERRIDE}.
     */
    public Builder() {}

    /**
     * Creates and initializes the builder with ACL information provided in {@code acl}.
     *
     * @param acl acl
     */
    public Builder(Acl acl) {
      readers = sanitizeSet(acl.getReaders());
      deniedReaders = sanitizeSet(acl.getDeniedReaders());
      owners = sanitizeSet(acl.getOwners());
      inheritFrom = acl.getInheritFrom();
      inheritFromFragment = acl.getInheritFromFragment();
      inheritType = acl.getInheritanceType();
    }

    private static ImmutableSet<Principal> sanitizeSet(Collection<Principal> principals) {
      if (principals.isEmpty()) {
        return ImmutableSet.of();
      }
      return ImmutableSet.copyOf(principals);
    }

    /**
     * Creates an immutable {@link Acl} instance of the current state.
     *
     * @return a fully formed {@link Acl}
     */
    public Acl build() {
      checkNotNull(readers, "readers can't be null");
      checkNotNull(deniedReaders, "deniedReaders can't be null");
      checkNotNull(owners, "owners can't be null");
      if (!Strings.isNullOrEmpty(inheritFrom)) {
        checkNotNull(inheritType, "inheritType can't be null when inheritFrom is defined");
      } else {
        inheritFrom = null;
        inheritFromFragment = null;
        inheritType = null;
      }
      return new Acl(this);
    }

    /**
     * Replaces existing readers.
     *
     * @param readers permit / readers
     * @return the same instance of the builder, for chaining calls
     * @throws NullPointerException if the collection is {@code null} or contains {@code null}
     * @throws IllegalArgumentException if the collection contains {@code ""} or a value that has
     *     leading or trailing whitespace
     */
    public Builder setReaders(Collection<Principal> readers) {
      this.readers = sanitizeSet(readers);
      return this;
    }

    /**
     * Replaces existing deniedReaders.
     *
     * @param deniedReaders deniedReaders
     * @return the same instance of the builder, for chaining calls
     * @throws NullPointerException if the collection is {@code null} or contains {@code null}
     * @throws IllegalArgumentException if the collection contains {@code ""} or a value that has
     *     leading or trailing whitespace
     */
    public Builder setDeniedReaders(Collection<Principal> deniedReaders) {
      this.deniedReaders = sanitizeSet(deniedReaders);
      return this;
    }

    /**
     * Replaces existing owners.
     *
     * @param owners owners
     * @return the same instance of the builder, for chaining calls
     * @throws NullPointerException if the collection is {@code null} or contains {@code null}
     * @throws IllegalArgumentException if the collection contains {@code ""} or a value that has
     *     leading or trailing whitespace
     */
    public Builder setOwners(Collection<Principal> owners) {
      this.owners = sanitizeSet(owners);
      return this;
    }

    /**
     * Sets the parent to inherit ACLs from.
     *
     * <p>Note that the parent's {@code InheritanceType} determines how to combine results with this
     * ACL.
     *
     * @param inheritFrom inherit from parent "id"
     * @return the same instance of the builder, for chaining calls
     * @see #setInheritanceType
     */
    public Builder setInheritFrom(String inheritFrom) {
      this.inheritFrom = inheritFrom;
      this.inheritFromFragment = null;
      return this;
    }

    /**
     * Sets the parent to inherit ACLs from.
     *
     * <p>Note that the parent's {@code InheritanceType} determines how to combine results with this
     * ACL.
     *
     * <p>The fragment facilitates a single parent id having multiple ACLs to inherit from. For
     * example, a single parent document id could have ACLs inherited by sub-folder instances and
     * different ACLs that are inherited by child files. The fragment allows specifying from
     * which of the parent's ACLs to inherited.
     *
     * @param inheritFrom inherit from parent id
     * @param fragment combined with {@code inheritFrom} to form the ACL
     * @return the same instance of the builder, for chaining calls
     * @see #setInheritanceType
     */
    public Builder setInheritFrom(String inheritFrom, String fragment) {
      this.inheritFrom = inheritFrom;
      this.inheritFromFragment = fragment;
      return this;
    }

    /**
     * Sets the type of Acl inheritance relationship between this id and any children.
     *
     * <p>ACL information is used to combine authorization decisions of this ACL with any of its
     * children based on the {@link InheritanceType}.
     *
     * @param inheritType inheritance type ({@link InheritanceType}
     * @return the same instance of the builder, for chaining calls
     * @throws NullPointerException if {@code inheritType} is {@code null}
     * @see #setInheritFrom
     */
    public Builder setInheritanceType(InheritanceType inheritType) {
      this.inheritType = checkNotNull(inheritType);
      return this;
    }
  }

  /**
   * Creates an {@link Item} from an id and fragment to be used for its ACL.
   *
   * @param id parent "id"
   * @param fragment associated fragment for the parent
   * @return an {@link Item} to be used as an ACL parent
   */
  public Item createFragmentItemOf(String id, String fragment) {
    Item item =
        new Item()
            .setName(fragmentId(id, fragment))
            .setItemType(ItemType.VIRTUAL_CONTAINER_ITEM.name());
    return applyTo(item);
  }

  /**
   * Applies current ACL information to the passed {@link Item}.
   *
   * @param item original {@link Item} to apply ACL information
   * @return the {@link Item} with the current ACL information applied
   */
  public Item applyTo(Item item) {
    checkNotNull(item, "Item cannot be null.");
    ItemAcl itemAcl =
        new ItemAcl()
            .setReaders(getClonedCopy(readers, Collectors.toList()))
            .setDeniedReaders(getClonedCopy(deniedReaders, Collectors.toList()))
            .setOwners(getClonedCopy(owners, Collectors.toList()));

    if (!Strings.isNullOrEmpty(inheritFrom)) {
      itemAcl
          .setInheritAclFrom(fragmentId(inheritFrom, inheritFromFragment))
          .setAclInheritanceType(inheritType.getCommonForm());
    }
    item.setAcl(itemAcl);
    return item;
  }

  /**
   * Creates a fragment id using the pattern {@code %s#%s}.
   *
   * @param id - document id
   * @param fragment - fragment name
   * @return created name
   */
  public static String fragmentId(String id, String fragment) {
    checkNotNull(id, "id can't be null");
    if (Strings.isNullOrEmpty(fragment)) {
      return id;
    }
    return String.format(FRAGMENT_ID_FORMAT, id, fragment);
  }

  /**
   * Creates principal name with pattern {@code %s:%s}.
   *
   * @param name - name
   * @param namespace - namespace
   * @return created name
   */
  public static String getPrincipalName(String name, String namespace) {
    checkNotNull(name, "name can't be null");
    if (Strings.isNullOrEmpty(namespace)) {
      return name;
    }
    return String.format("%s:%s", namespace, name);
  }

  /**
   * Returns a Google user principal.
   *
   * @param userId Google user ID
   * @return {@link Principal} with kind set to Google user
   */
  public static Principal getGoogleUserPrincipal(String userId) {
    checkArgument(!Strings.isNullOrEmpty(userId), "Google user ID can not be empty or null");
    return new Principal().setGsuitePrincipal(new GSuitePrincipal().setGsuiteUserEmail(userId));
  }

  /**
   * Returns a Google group principal.
   *
   * @param groupId external group ID
   * @return {@link Principal} with kind set to Google group
   */
  public static Principal getGoogleGroupPrincipal(String groupId) {
    checkArgument(!Strings.isNullOrEmpty(groupId), "Google group ID can not be empty or null");
    return new Principal().setGsuitePrincipal(new GSuitePrincipal().setGsuiteGroupEmail(groupId));
  }

  /**
   * Returns a customer principal instance.
   *
   * @return {@link Principal} with kind as Customer
   */
  public static Principal getCustomerPrincipal() {
    return new Principal().setGsuitePrincipal(new GSuitePrincipal().setGsuiteDomain(true));
  }

  /**
   * Returns an external user principal.
   *
   * @param userId external user ID
   * @return {@link Principal} with kind as external user
   */
  public static Principal getUserPrincipal(String userId) {
    checkArgument(!Strings.isNullOrEmpty(userId), "User ID can not be empty or null");
    return new Principal().setUserResourceName(userId);
  }

  /**
   * Returns an external user principal under specified identity source ID.
   *
   * @param userId external user ID
   * @param identitySourceId identity source ID for external user principal
   * @return {@link Principal} with kind as external user
   */
  public static Principal getUserPrincipal(String userId, String identitySourceId) {
    checkArgument(!Strings.isNullOrEmpty(userId), "User ID can not be empty or null");
    checkArgument(
        !Strings.isNullOrEmpty(identitySourceId), "identity source ID can not be empty or null");
    return new Principal()
        .setUserResourceName(String.format(USER_RESOURCE_NAME_FORMAT, identitySourceId, userId));
  }

  private static Supplier<Set<String>> externalGroupNamesSupplier = () -> {
    try {
      ExternalGroups groups = ExternalGroups.fromConfiguration();
      return groups.getExternalGroups().stream()
      .map(group -> group.getName())
      .collect(ImmutableSet.toImmutableSet());
    } catch (IOException e) {
      return ImmutableSet.of();
    }
  };

  private static Supplier<Set<String>> externalGroupNames = memoize(externalGroupNamesSupplier);

  private static Supplier<String> externalGroupsIdentitySourceIdSupplier = () -> {
    return Configuration.getString("externalgroups.identitySourceId", "").get();
  };

  private static Supplier<String> externalGroupsIdentitySourceId =
      memoize(externalGroupsIdentitySourceIdSupplier);

  /** TestRule to reset the static cached external groups data for tests. */
  public static class ResetExternalGroupsRule implements TestRule {
    @Override
    public Statement apply(Statement base, Description description) {
      externalGroupsIdentitySourceId = memoize(externalGroupsIdentitySourceIdSupplier);
      externalGroupsIdentitySourceId = memoize(externalGroupsIdentitySourceIdSupplier);
      return base;
    }
  }

  /**
   * Returns an external group principal. This method encodes groupId using {@link
   * GroupIdEncoder#encodeGroupId}
   *
   * @param groupId external user ID
   * @return {@link Principal} with kind as external group
   */
  public static Principal getGroupPrincipal(String groupId) {
    checkArgument(!Strings.isNullOrEmpty(groupId), "Group ID can not be empty or null");
    if (Configuration.isInitialized()
        && !externalGroupsIdentitySourceId.get().isEmpty()
        && externalGroupNames.get().contains(groupId)) {
      return getGroupPrincipal(groupId, externalGroupsIdentitySourceId.get());
    }
    return new Principal().setGroupResourceName(GroupIdEncoder.encodeGroupId(groupId));
  }

  /**
   * Returns an external group principal under specified identity source ID. This method encodes
   * groupId using {@link GroupIdEncoder#encodeGroupId}
   *
   * @param groupId external user ID
   * @param identitySourceId identity source ID for external group principal
   * @return {@link Principal} with kind as external group
   */
  public static Principal getGroupPrincipal(String groupId, String identitySourceId) {
    checkArgument(!Strings.isNullOrEmpty(groupId), "Group ID can not be empty or null");
    checkArgument(
        !Strings.isNullOrEmpty(identitySourceId), "identity source ID can not be empty or null");
    return new Principal()
        .setGroupResourceName(
            String.format(
                GROUP_NAME_FORMAT, identitySourceId, GroupIdEncoder.encodeGroupId(groupId)));
  }

  /**
   * Adds resource prefix "identitysources/identitySourceId/users/" to {@link
   * Principal#setUserResourceName}.
   *
   * @param user principal to add resource prefix for.
   * @param identitySourceId identity source ID for external user principal.
   * @return true if resource prefix is added to principal, false otherwise.
   */
  public static boolean addResourcePrefixUser(Principal user, String identitySourceId) {
    checkNotNull(user);
    String userName = user.getUserResourceName();
    checkArgument(!Strings.isNullOrEmpty(userName), "userName can not be empty or null");
    if (userName.startsWith(IDENTITY_SOURCES_PREFIX)) {
      return false;
    }
    user.setUserResourceName(String.format(USER_RESOURCE_NAME_FORMAT, identitySourceId, userName));
    return true;
  }

  /**
   * Adds resource prefix "identitysources/identitySourceId/groups/" to {@link
   * Principal#setGroupResourceName}. This method assumes that {@link
   * Principal#getGroupResourceName} is already encoded for escaping unsupported characters.
   *
   * @param group principal to add resource prefix for.
   * @param identitySourceId identity source ID for external group principal.
   * @return true if resource prefix is added to principal, false otherwise.
   */
  public static boolean addResourcePrefixGroup(Principal group, String identitySourceId) {
    checkNotNull(group);
    String groupName = group.getGroupResourceName();
    checkArgument(!Strings.isNullOrEmpty(groupName), "groupName can not be empty or null");
    if (groupName.startsWith(IDENTITY_SOURCES_PREFIX)) {
      return false;
    }
    group.setGroupResourceName(String.format(GROUP_NAME_FORMAT, identitySourceId, groupName));
    return true;
  }

  /** Represents type of a {@link Principal} */
  public static enum PrincipalType {
    /** Google user principal */
    GSUITE_USER,
    /** Google group principal */
    GSUITE_GROUP,
    /** G suite domain or customer principal */
    GSUITE_DOMAIN,
    /** External user principal */
    USER,
    /** External group principal */
    GROUP;
  }

  /**
   * Returns {@link PrincipalType} for given principal
   *
   * @param p principal to compute {@link PrincipalType} for
   * @return {@link PrincipalType} for given principal
   */
  public static PrincipalType getPrincipalType(Principal p) {
    checkNotNull(p, "Principal can not be null");
    boolean isGSuitePrincipal = Objects.nonNull(p.getGsuitePrincipal());
    boolean isGSuiteUser =
        isGSuitePrincipal && !Strings.isNullOrEmpty(p.getGsuitePrincipal().getGsuiteUserEmail());
    boolean isGSuiteGroup =
        isGSuitePrincipal && !Strings.isNullOrEmpty(p.getGsuitePrincipal().getGsuiteGroupEmail());
    boolean isGSuiteDomain =
        isGSuitePrincipal
            && Objects.nonNull(p.getGsuitePrincipal().getGsuiteDomain())
            && p.getGsuitePrincipal().getGsuiteDomain();
    boolean isUserPrincipal = !Strings.isNullOrEmpty(p.getUserResourceName());
    boolean isGroupPrincipal = !Strings.isNullOrEmpty(p.getGroupResourceName());
    if (isGSuitePrincipal) {
      checkArgument(!isUserPrincipal && !isGroupPrincipal, "Invalid principal");
      if (isGSuiteGroup) {
        checkArgument(!isGSuiteDomain && !isGSuiteUser, "Invalid principal");
        return PrincipalType.GSUITE_GROUP;
      } else if (isGSuiteUser) {
        checkArgument(!isGSuiteDomain, "Invalid principal");
        return PrincipalType.GSUITE_USER;
      } else {
        checkArgument(isGSuiteDomain, "Invalid principal");
        return PrincipalType.GSUITE_DOMAIN;
      }
    } else if (isUserPrincipal) {
      checkArgument(!isGroupPrincipal, "Invalid principal");
      return PrincipalType.USER;
    } else {
      checkArgument(isGroupPrincipal, "Invalid principal");
      return PrincipalType.GROUP;
    }
  }

  /** The enum that presents the type of inheritance. */
  public static enum InheritanceType {
    /**
     * The inherit from (parent's) ACL takes precedence over the {@link Item} (child's) ACL.
     */
    PARENT_OVERRIDE("PARENT_OVERRIDE"),
    /**
     * The {@link Item} (child's) ACL takes precedence over the inherit from (parent's) ACL.
     */
    CHILD_OVERRIDE("CHILD_OVERRIDE"),
    /**
     * Both the current (child's) and inherit from (parent's) ACLs must both allow access.
     */
    BOTH_PERMIT("BOTH_PERMIT");

    private final String commonForm;

    private InheritanceType(String commonForm) {
      this.commonForm = commonForm;
    }

    /** The identifier used to represent enum value during communication with the server. */
    String getCommonForm() {
      return commonForm;
    }
  }

  /**
   * Creates an {@link Acl} from comma delimited strings.
   *
   * @param permittedUsers readers
   * @param permittedGroups permitted groups
   * @param deniedUsers denied readers
   * @param deniedGroups denied groups
   * @return fully formed {@link Acl}
   */
  public static Acl createAcl(String permittedUsers, String permittedGroups, String deniedUsers,
      String deniedGroups) {
    return buildAcl(splitList(permittedUsers), splitList(permittedGroups),
        splitList(deniedUsers), splitList(deniedGroups));
  }

  /**
   * Splits a comma delimited string into a list.
   *
   * @param fields comma separated string
   * @return list of the fields
   */
  private static List<String> splitList(String fields) {
    return Splitter.on(',').trimResults().omitEmptyStrings().splitToList(fields);
  }

  /**
   * Builds an Acl with the passed parameters.
   *
   * @param permittedUsers permitted readers
   * @param permittedGroups permitted groups
   * @param deniedUsers denied readers
   * @param deniedGroups denied groups
   * @return fully formed Acl
   */
  private static Acl buildAcl(List<String> permittedUsers, List<String> permittedGroups,
      List<String> deniedUsers, List<String> deniedGroups) {
    List<Principal> readers = new ArrayList<>();
    List<Principal> deniedReaders = new ArrayList<>();
    addUsers(readers, permittedUsers);
    addGroups(readers, permittedGroups);
    addUsers(deniedReaders, deniedUsers);
    addGroups(deniedReaders, deniedGroups);
    return new Acl.Builder().setReaders(readers).setDeniedReaders(deniedReaders).build();
  }

  /**
   * Adds user {@link Principal} objects to a list of either readers or denied readers.
   *
   * @param principals readers or denied readers
   * @param users added Google or External users
   */
  private static void addUsers(List<Principal> principals, List<String> users) {
    for (String user : users) {
      principals.add(USER_PARSER.parse(user));
    }
  }

  /**
   * Adds group {@link Principal} objects to a list of either readers or denied readers.
   *
   * @param principals readers or denied readers
   * @param groups added Google or External groups
   */
  private static void addGroups(List<Principal> principals, List<String> groups) {
    for (String group : groups) {
      principals.add(GROUP_PARSER.parse(group));
    }
  }

  private static <T extends Collection<Principal>> T getClonedCopy(
      ImmutableSet<Principal> principals, Collector<Principal, ?, T> collector) {
    checkNotNull(principals, "principals can not be null");
    return principals
        .stream()
        .map(Principal::clone)
        .collect(collector);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        inheritFrom, inheritType, inheritFromFragment, readers, deniedReaders, owners);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Acl)) {
      return false;
    }
    Acl other = (Acl) obj;
    return Objects.equals(inheritFrom, other.inheritFrom)
        && Objects.equals(inheritType, other.inheritType)
        && Objects.equals(inheritFromFragment, other.inheritFromFragment)
        && Objects.equals(readers, other.readers)
        && Objects.equals(deniedReaders, other.deniedReaders)
        && Objects.equals(owners, other.owners);
  }

  @Override
  public String toString() {
    return "Acl [readers="
        + readers
        + ", deniedReaders="
        + deniedReaders
        + ", owners="
        + owners
        + ", inheritFrom="
        + inheritFrom
        + ", inheritFromFragment="
        + inheritFromFragment
        + ", inheritType="
        + inheritType
        + "]";
  }

  /**
   * Parser used to extract a user principal of either "google" or "external" type.
   *
   * <p>By definition, accounts beginning with {@value
   * com.google.enterprise.cloudsearch.sdk.indexing.Acl#GOOGLE_ACCOUNT_PREFIX} are "google"
   * accounts.
   */
  public static class UserPrincipalParser implements Configuration.Parser<Principal> {
    /**
     * Converts {@code user} value to {@link Principal} representing external user or Google user.
     */
    @Override
    public Principal parse(String user) throws InvalidConfigurationException {
      checkArgument(!Strings.isNullOrEmpty(user), "user can not be null or empty");
      if (user.startsWith(GOOGLE_ACCOUNT_PREFIX)) {
        return Acl.getGoogleUserPrincipal(user.substring(GOOGLE_ACCOUNT_PREFIX.length()));
      } else {
        return Acl.getUserPrincipal(user);
      }
    }
  }

  /**
   * Parser used to extract a group principal of either "google" or "external" type.
   *
   * <p>By definition, accounts beginning with {@value
   * com.google.enterprise.cloudsearch.sdk.indexing.Acl#GOOGLE_ACCOUNT_PREFIX} are "google"
   * accounts.
   */
  public static class GroupPrincipalParser implements Configuration.Parser<Principal> {
    /**
     * Converts {@code group} value to {@link Principal} representing external group or Google
     * group.
     */
    @Override
    public Principal parse(String group) throws InvalidConfigurationException {
      checkArgument(!Strings.isNullOrEmpty(group), "group can not be null or empty");
      if (group.startsWith(GOOGLE_ACCOUNT_PREFIX)) {
        return Acl.getGoogleGroupPrincipal(group.substring(GOOGLE_ACCOUNT_PREFIX.length()));
      } else {
        return Acl.getGroupPrincipal(group);
      }
    }
  }
}
