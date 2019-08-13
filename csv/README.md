# Google Cloud Search CSV Connector

The Google Cloud Search CSV connector works with any comma-separated values (CSV) file, that is a
delimited text file that uses a comma to separate values.

Before running the CSV connector, you should review the [access control list options](https://developers.google.com/cloud-search/docs/guides/csv-connector#9_specify_access_control_list_acl_options).


## Build instructions

1. Build the connector

   a. Clone the SDK repository from GitHub:
      ```
      git clone https://github.com/google-cloudsearch/connector-sdk.git
      cd connector-sdk/csv
      ```

   b. Checkout the desired version of the SDK and build the ZIP file:
      ```
      git checkout tags/v1-0.0.5
      mvn package
      ```
      (To skip the tests when building the connector, use `mvn package -DskipTests`)


2. Install the connector

   The `mvn package` command creates a ZIP file containing the
   connector and its dependencies with a name like
   `google-cloudsearch-csv-connector-v1-0.0.5.zip`.

   a. Copy this ZIP file to the location where you want to install the connector.

   b. Unzip the connector ZIP file. A directory with a name like
      `google-cloudsearch-csv-connector-v1-0.0.5` will be created.

   c. Change into this directory. You should see the connector jar file,
      `google-cloudsearch-csv-connector-v1-0.0.5.jar`, as well as a `lib`
      directory containing the connector's dependencies.


3. Configure the connector

   a. Create a file containing the connector configuration parameters. Refer to the
   [configuration documentation](https://developers.google.com/cloud-search/docs/guides/csv-connector#2_specify_the_csv_connector_configuration)
   for specifics and for parameter details.


4. Run the connector

   The connector should be run from the unzipped installation directory, **not** the source
   code's `target` directory.

   ```
   java \
      -jar google-cloudsearch-csv-connector-v1-0.0.5.jar \
      -Dconfig=my.config
   ```

   Where `my.config` is the configuration file containing the parameters for the connector
   execution.

   **Note:** If the configuration file is not specified, a default file name
   `connector-config.properties` will be assumed. Refer to
   [configuration documentation](https://developers.google.com/cloud-search/docs/guides/csv-connector#specify-configuration)
   for specifics and for parameter details.

For further information on configuration and deployment of this connector, see
[Deploy a CSV connector](https://developers.google.com/cloud-search/docs/guides/csv-connector).
