# Google Cloud Search CSV Connector

The Google Cloud Search CSV connector works with any comma-separated values (CSV) file, that is a
delimited text file that uses a comma to separate values.

## Build instructions

1. Install the SDK into your local Maven repository and build the connector

   a. Clone the SDK repository from GitHub:
      ```
       git clone https://github.com/google-cloudsearch/connector-sdk.git
       cd connector-sdk
      ```

   b. Checkout the desired version of the SDK:
      ```
      git checkout tags/v1-0.0.5
      ```

   c. Install the SDK components:
      ```
      mvn install
      ```

2. Run the connector
   ```
   java \
      -jar csv/target/google-cloudsearch-csv-connector-v1-0.0.5.jar \
      -Dconfig=my.config
   ```

   Where `my.config` is the configuration file containing the parameters for the connector
   execution.

   **Note:** If the configuration file is not specified, a default file name
   `connector-config.properties` will be assumed. Refer to
   [configuration documentation](https://developers.google.com/cloud-search/docs/guides/csv-connector#specify-configuration)
   for specifics and for parameter details.

3. Install the connector

   To install the connector for testing or production, copy the ZIP file from the
   target directory to the desired machine and unzip it in the desired directory.

For further information on configuration and deployment of this connector, see
[Deploy a CSV connector](https://developers.google.com/cloud-search/docs/guides/csv-connector).
