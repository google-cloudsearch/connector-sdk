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
package com.google.enterprise.cloudsearch.csvconnector;

import com.google.enterprise.cloudsearch.sdk.indexing.IndexingApplication;
import com.google.enterprise.cloudsearch.sdk.indexing.template.FullTraversalConnector;

/**
 * CSVConnector uses {@link FullTraversalConnector} to send csv records to the indexing API. It will
 * invoke {@link CSVRepository} to iterate through the csv records. {@link CSVFileManager} reads in
 * the csv file and translates each csv record to {@link
 * com.google.api.services.cloudsearch.v1.model.Item} which will be sent to the indexing API.
 *
 * <p>To read in a csv file properly, several configuration properties must be defined. The detailed
 * explanation is in {@link CSVFileManager}. A sample configuration properties file can be found at
 * {src/main/resources/com/google/enterprise/cloudsearch/csvconnector/csvconnector-config.properties.sample}
 *
 * <p>A custom schema is supported to improve search quality. It describes how to use the data sent
 * to the indexing API. A sample glossary schema json file can be found at
 * {src/main/resources/com/google/enterprise/cloudsearch/csvconnector/glossarySchema.json}
 *
 * <p>Command to run the csv connector:
 *
 * <pre>
 *    java
 *    -jar google-cloudsearch-csv-connector-{version}.jar
 *    -Dconfig={your config.properties file}
 *  </pre>
 *
 * If the configuration file is not specified, the connector will use the
 * connector-config.properties file by default.
 */
public class CSVConnector {
  public static void main(String[] args) throws InterruptedException {
    IndexingApplication application = new IndexingApplication.Builder(
        new FullTraversalConnector(new CSVRepository()), args).build();
    application.start();
  }
}
