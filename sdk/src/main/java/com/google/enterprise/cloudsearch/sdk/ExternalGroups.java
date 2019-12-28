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

package com.google.enterprise.cloudsearch.sdk;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Key;
import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents external group data. Data is loaded from a JSON file.
 * <p>
 * The group names provided here should match the values that might occur in source
 * repository ACLs when indexing content. For example, if a repository has a group named
 * "Everyone" used for access control that's not otherwise synced to the Google directory,
 * then you'd create an entry here for "Everyone". Group names in the JSON file should not
 * be escaped; the names will be escaped as needed for the Google directory when the
 * groups are created.
 * <p>
 * Use the configuration property <code>externalgroups.filename</code> to specify the file
 * containing the data.
 *
 * <pre>
 *   {"externalGroups":[
 *     {"name":"group-name",
 *      "members":[
 *        {"id":"member1"},
 *        {"id":"member2", "namespace":"identitysources/1234567899"}
 *      ]
 *     }
 *    ]
 *   }
 * </pre>
 *
 * If a member is specified using only an id, that id should correspond to a Google user
 * or group in the domain. If a member is specified with a namespace, that id should
 * correspond to an id within the given namespace.
 */
public class ExternalGroups extends GenericJson {
  private static final Logger logger = Logger.getLogger(ExternalGroups.class.getName());

  public static final String CONFIG_EXTERNALGROUPS_FILENAME =
      "externalgroups.filename";

  public static ExternalGroups fromConfiguration() throws IOException {
    checkState(Configuration.isInitialized(), "configuration must be initialized");
    String groupsFilename = Configuration.getString(CONFIG_EXTERNALGROUPS_FILENAME, null).get();
    logger.log(Level.CONFIG, CONFIG_EXTERNALGROUPS_FILENAME + ": " + groupsFilename);
    File groupsFile = new File(groupsFilename);
    if (!(groupsFile.exists() && groupsFile.canRead())) {
      throw new InvalidConfigurationException(groupsFilename + " can't be read");
    }
    return fromFile(groupsFile);
  }

  public static ExternalGroups fromFile(File groupsFile) throws IOException {
    // fromReader is documented to close the provided stream.
    return JacksonFactory.getDefaultInstance().fromReader(
        new InputStreamReader(new FileInputStream(groupsFile), UTF_8), ExternalGroups.class);
  }

  @Key
  public List<ExternalGroup> externalGroups;

  public List<ExternalGroup> getExternalGroups() {
    if (externalGroups == null) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(externalGroups);
  }

  /**
   * A single group.
   */
  public static class ExternalGroup extends GenericJson {
    @Key
    public String name;

    @Key
    public List<MemberKey> members;

    public String getName() {
      return name;
    }

    public List<MemberKey> getMembers() {
      if (members == null) {
        return ImmutableList.of();
      }
      return ImmutableList.copyOf(members);
    }
  }

  /**
   * The identity information for a member.
   */
  public static class MemberKey extends GenericJson {
    @Key
    public String id;

    @Key
    public String namespace;

    @Key
    public String referenceIdentitySourceName;

    public String getId() {
      return id;
    }

    public String getNamespace() {
      return namespace;
    }

    public String getReferenceIdentitySourceName() {
      return referenceIdentitySourceName;
    }
  }
}
