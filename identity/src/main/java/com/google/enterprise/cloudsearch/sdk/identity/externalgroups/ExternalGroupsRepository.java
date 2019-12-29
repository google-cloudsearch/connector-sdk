/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.enterprise.cloudsearch.sdk.identity.externalgroups;

import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.api.services.cloudidentity.v1.model.Membership;
import com.google.api.services.cloudidentity.v1.model.MembershipRole;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.ExternalGroups;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.identity.IdentityGroup;
import com.google.enterprise.cloudsearch.sdk.identity.IdentitySourceConfiguration;
import com.google.enterprise.cloudsearch.sdk.identity.IdentityUser;
import com.google.enterprise.cloudsearch.sdk.identity.Repository;
import com.google.enterprise.cloudsearch.sdk.identity.RepositoryContext;
import java.io.IOException;
import java.util.Collections;
import java.util.logging.Logger;

/**
 * Indexes external groups and their members.
 * <p>
 * Example config to run the external groups connector:
 * <pre>
 * api.serviceAccountPrivateKeyFile = /path/to/file
 * api.customerId = ABCDE123
 * connector.runOnce = true
 * ## This property must be set to GROUPS to avoid an error trying to sync users
 * connector.IdentitySyncType = GROUPS
 * ## This is the identity source in which the groups will be created. This must be a
 * ## dedicated identity source for this connector.
 * api.identitySourceId = 1234567890abcde
 * externalgroups.filename = /path/to/groups.json
 * </pre>
 *
 * <p>
 * Mapping an external group to the customer principal (all users) is a special case. You
 * need to create a Google group containing the customer principal and then set that Google
 * group as the member of the external group to be created here. For example:
 * <pre>
 * {
 *     "externalGroups":[
 *         {"name":"Everyone",
 *          "members":[
 *              {"id":"everyone-group@example.com"}
 *          ]
 *         }
 *     ]
 * }
 * </pre>
 *
 * <p>
 * In a content connector, you need to supply two properties to map external group names
 * present in the source repository ACLs to the groups created in the dedicated identity
 * source:
 * <pre>
 * externalgroups.identitySourceId = 1234567890abcde
 * externalgroups.filename = /path/to/groups.json
 * </pre>
 */
public class ExternalGroupsRepository implements Repository {
  private static final Logger logger = Logger.getLogger(ExternalGroupsRepository.class.getName());

  private RepositoryContext repositoryContext;
  private final GroupsReader groupsReader;

  public ExternalGroupsRepository() {
    this(ExternalGroups::fromConfiguration);
  }

  @VisibleForTesting
  ExternalGroupsRepository(GroupsReader groupsReader) {
    this.groupsReader = groupsReader;
  }

  @Override
  public void init(RepositoryContext context) throws IOException {
    repositoryContext = context;
  }

  // TODO: do we want to support separating display name from group name?
  @Override
  public CheckpointCloseableIterable<IdentityGroup> listGroups(byte[] checkpoint)
      throws IOException {
    ExternalGroups groups = groupsReader.getGroups();
    ImmutableList.Builder<IdentityGroup> groupsBuilder = ImmutableList.builder();
    for (ExternalGroups.ExternalGroup group : groups.getExternalGroups()) {
      ImmutableSet.Builder<Membership> membersBuilder = ImmutableSet.builder();
      for (ExternalGroups.MemberKey memberKey : group.getMembers()) {
        EntityKey entityKey = new EntityKey().setId(memberKey.getId());
        if (memberKey.getNamespace() != null) {
          entityKey.setNamespace(memberKey.getNamespace());
        } else if (memberKey.getReferenceIdentitySourceName() != null) {
          String sourceName = memberKey.getReferenceIdentitySourceName();
          IdentitySourceConfiguration config = IdentitySourceConfiguration
              .getReferenceIdentitySourcesFromConfiguration().get(sourceName);
          if (config == null) {
            throw new InvalidConfigurationException(
                "Missing config for reference identity source " + sourceName);
          }
          entityKey.setNamespace(config.getGroupNamespace());
        } // else id is assumed to be a Google id
        membersBuilder.add(new Membership()
            .setPreferredMemberKey(entityKey)
            .setRoles(Collections.singletonList(new MembershipRole().setName("MEMBER")))
          );
      }
      groupsBuilder.add(
          repositoryContext.buildIdentityGroup(group.getName(), membersBuilder::build));
    }
    return new CheckpointCloseableIterableImpl.Builder<IdentityGroup>(groupsBuilder.build())
        .build();
  }

  @FunctionalInterface
  interface GroupsReader {
    ExternalGroups getGroups() throws IOException;
  }

  @Override
  public CheckpointCloseableIterable<IdentityUser> listUsers(byte[] checkpoint) throws IOException {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public void close() {
  }
}
