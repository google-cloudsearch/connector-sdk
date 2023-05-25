# Google Cloud Search Connector SDK

Google Cloud Search connectors are software programs allowing Google Cloud Search to operate on
third-party data. The Content Connector SDK is a wrapper around the REST API allowing one to
quickly create connectors.

There are two types of connectors: content connectors and identity connectors. Content connectors
are used to traverse a repository and index the data so that Google Cloud Search can effectively
search that data. For more information on building content connectors using Google Cloud Search
connector SDK refer to https://developers.google.com/cloud-search/docs/guides/content-connector.

Identity connectors are used to map your enterprise's identities and group rosters to the Google
accounts and groups used by Google Cloud Search. For more information on building identity
connectors using Google Cloud Search connector SDK, refer to
https://developers.google.com/cloud-search/docs/guides/identity-connector.

## Build instructions

Building the Google Cloud Search Connector SDK requires the following development tools:
  - Java SE Development Kit (JDK) version 1.8.0_20 or greater
  - Apache Maven version 3.3.0 or greater.

1. Clone the SDK repository from GitHub:
   ```
   git clone https://github.com/google-cloudsearch/connector-sdk.git
   cd connector-sdk
   ```

2. Checkout the desired version of the SDK:
   ```
   git checkout tags/v1-0.0.5
   ```

3. Install the SDK components:
   ```
   mvn install
   ```

Maven commands:
-  `mvn install` - download required dependencies, build and install sdk jar as local maven repository
-  `mvn clean` - deletes generated "target" directories.
-  `mvn compile` - compiles source.
-  `mvn test` - runs tests.
-  `mvn package` - compiles, tests, and creates the jar files and a distribution zip file.
-  `mvn -Pjavadoc` package - `mvn package` with `target/javadoc` and `-javadoc.jar` file(s).

## Running integration tests

The integration tests check the correct interoperability between the connector SDK and the
Cloud Search APIs. They are run using the Failsafe plug-in and follow its naming conventions (i.e.,
they are suffixed with "IT").

To run them, build and install the SDK, then `cd` into the `it/` subdirectory, and issue the
command

```
mvn integration-test \
    -DskipITs=false \
    -DargLine=-Dapi.test.serviceAccountPrivateKeyFile=<PATH_TO_SERVICE_ACCOUNT_KEY> \
    -Dapi.test.sourceId=<DATA_SOURCE_ID> \
    -Dapi.test.identitySourceId=<IDENTITY_SOURCE_ID> \
    -Dapi.test.customerId=<CUSTOMER_ID> \
    -Dapi.test.domain=<COMPANY_DOMAIN> \ 
    -Dapi.test.adminUserId=<ADMIN_EMAIL_ID> 
    -Dapi.test.rootUrl=<ROOT_URL> \
    -Dapi.test.searchApplicationId=<SEARCH_APPLICATION_ID> \
    -Dapi.test.authInfoUser1=<USER1_EMAIL_ID>,<PATH_TO_FOLDER_CONTAINING_USER1_STORED_CREDENTIALS>,<PATH_TO_JSON_CONTAINING_CLIENT_SECRET_FOR_DESKTOP_APP> \
    -Dapi.test.authInfoUser2=<USER2_EMAIL_ID>,<PATH_TO_FOLDER_CONTAINING_USER2_STORED_CREDENTIALS>,<PATH_TO_JSON_CONTAINING_CLIENT_SECRET_FOR_DESKTOP_APP> \ 
    -Dapi.test.authInfoUser3=<USER3_EMAIL_ID>,<PATH_TO_FOLDER_CONTAINING_USER3_STORED_CREDENTIALS>,<PATH_TO_JSON_CONTAINING_CLIENT_SECRET_FOR_DESKTOP_APP> \ 
    -Dapi.test.authInfo=<ADMIN_EMAIL_ID>,<PATH_TO_FOLDER_CONTAINING_ADMIN_STORED_CREDENTIALS>,<PATH_TO_JSON_CONTAINING_CLIENT_SECRET_FOR_DESKTOP_APP> \
    -Dapi.test.groupPrefix=<GROUP_ID>
```

where

- `api.test.serviceAccountKey`: path to JSON file containing the credentials of a service account
  that can access the APIs.

- `api.test.sourceId` is the ID of the datasource to which the test will sync
  the data. The datasource must have its schema as the schema present in `it/schema.protoascii`.

- `api.test.identitySourceId`: ID of a Cloud Identity source. Preferably, this should be
  a source only used for testing. Temporary groups will be created on it during the execution of
  the test.

- `api.test.customerId`: the customer ID.

- `api.test.domain`: the domain associated with a Cloud Identity customer. Temporary users
  will be created on this domain during the execution of the test.

- `api.test.adminUserId`: Email ID of admin account.

- `api.test.rootUrl`: the base url of api endpoint. For prod the `rootUrl` is https://cloudsearch.googleapis.com/. For staging the `rootUrl` is https://staging-cloudsearch.sandbox.googleapis.com/.

- `api.test.searchApplicationId`: the search application through which all search requests will be sent. The argument should be of the form `searchapplications/<SEARCH_APPLICATION_ID>`. The search application must contain the created datasource above in its datasource restrictions.

- `api.test.authInfoUser1`: Authentication information for User1. This consists of the email id of User1, the path to the folder containing the user's Stored Credentials and the path to the JSON file containing client secret for desktop app.\
  NOTE: The Stored Credential of the user must be kept in a file called StoredCredential, and we only need to pass the path of the folder containing the file.

- `api.test.authInfoUser2`: Authentication information for User2. This consists of the email id of User2, the path to the folder containing the user's Stored Credentials and the path to the JSON file containing client secret for desktop app.\
  NOTE: The Stored Credential of the user must be kept in a file called StoredCredential, and we only need to pass the path of the folder containing the file.

- `api.test.authInfoUser3`: Authentication information for User3. This consists of the email id of User3, the path to the folder containing the user's Stored Credentials and the path to the JSON file containing client secret for desktop app.\
  NOTE: The Stored Credential of the user must be kept in a file called StoredCredential, and we only need to pass the path of the folder containing the file.

- `api.test.authInfo`: Authentication information for Admin. This consists of the email id of Admin, the path to the folder containing the admin's Stored Credentials and the path to the JSON file containing client secret for desktop app.\
  NOTE: The Stored Credential of the admin must be kept in a file called StoredCredential, and we only need to pass the path of the folder containing the file.

- `api.test.groupPrefix` : The name of a group created for testing purpose. This group must contain User3 and should not contain User1 and User2.

NOTE: User1, User2 and User3 should all be different. We can set one the users as the Admin if needed.

It's necessary to set a specific schema for the tests to run, otherwise a check
during the set-up of the tests will fail. The schema can be found in
it/schema.json (or it/schema.protoascii).

The following is an example of a complete command:

```
mvn integration-test \
    -DskipITs=false \
    -DargLine=-Dapi.test.serviceAccountPrivateKeyFile=key.json \
    -Dapi.test.sourceId=9d390ed09e9e54c1108b40e137ce70c0 \
    -Dapi.test.identitySourceId=9d390ed09e9e54c1313ed6e825c27a7c \
    -Dapi.test.customerId=C03z1stam \
    -Dapi.test.domain=topazauditlog01.bigr.name \
    -Dapi.test.adminUserId=admin@topazauditlog01.bigr.name \
    -Dapi.test.rootUrl=https://staging-cloudsearch.sandbox.googleapis.com/ \
    -Dapi.test.searchApplicationId=searchapplications/9d390ed09e9e54c1d7d01059854e17df \
    -Dapi.test.authInfoUser2=user1@topazauditlog01.bigr.name,/Users/sarthakkaps/change1/connector-sdk/it/user1,/Users/sarthakkaps/change1/connector-sdk/it/client_secret_user1_topazauditlog01.bigr.json \
    -Dapi.test.authInfoUser1=admin@topazauditlog01.bigr.name,/Users/sarthakkaps/change1/connector-sdk/it/admin,/Users/sarthakkaps/change1/connector-sdk/it/client_secret_admin_topazauditlog01.bigr.json \
    -Dapi.test.authInfoUser3=user2@topazauditlog01.bigr.name,/Users/sarthakkaps/change1/connector-sdk/it/user2,/Users/sarthakkaps/change1/connector-sdk/it/client_secret_user2_topazauditlog01.bigr.json \
    -Dapi.test.authInfo=admin@topazauditlog01.bigr.name,/Users/sarthakkaps/change1/connector-sdk/it/admin,/Users/sarthakkaps/change1/connector-sdk/it/client_secret_admin_topazauditlog01.bigr.json \
    -Dapi.test.groupPrefix=connector_sdk_test_group
```

Adding `-Dit.test=<IT class>#<method>` to the command line allows to run only a specific
test.

Notice also that will it's possible to run only unit tests (using
`-DskipITs=true`) it's not obvious how to run *only* integration tests
(`-DskipTests=true` skips both unit and integration tests). A work-around
for this is specifying `-Dtest=NONE -DfailIfNoTests=false` (assuming that
there's no test class called `NONE`) plus the appropriate flags to run
integration tests. For example:

```
mvn verify \
    -Dtest=NONE -DfailIfNoTests=false \
    -DskipITs=false \
    -Dit.test=SomeTest \
    ...
```

## Code coverage

A code coverage report for unit tests is generated for each module as part of the "test" goal. The
output directory for the report is `<module>/target/site/jacoco/`.
