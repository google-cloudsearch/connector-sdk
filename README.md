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
mvn verify \
    -DskipITs=false \
    -Dapi.test.serviceAccountPrivateKeyFile=<PATH_TO_SERVICE_ACCOUNT_KEY> \
    -Dapi.test.sourceId=<DATA_SOURCE_ID>
    -Dapi.test.identitySourceId=<IDENTITY_SOURCE_ID> \
    -Dapi.test.customerId=<CUSTOMER_ID> \
    -Dapi.test.domain=<COMPANY_DOMAIN>
```

where

- `api.test.serviceAccountKey`: path to JSON file containing the credentials of a service account
  that can access the APIs.

- `api.test.sourceId` is the ID of the data source to which the test will sync
  the data.

- `api.test.identity.identitySourceId`: ID of a Cloud Identity source. Preferably, this should be
  a source only used for testing. Temporary groups will be created on it during the execution of
  the test.

- `api.test.identity.customerId`: the customer ID.

- `api.test.identity.domain`: the domain associated with a Cloud Identity customer. Temporary users
  will be created on this domain during the execution of the test.

It's necessary to set a specific schema for the tests to run, otherwise a check
during the set-up of the tests will fail. The schema can be found in
it/schema.json (or it/schema.protoascii).

The following is an example of a complete command:

```
mvn verify \
    -DskipITs=false \
    -Dapi.test.serviceAccountPrivateKeyFile=key.json \
    -Dapi.test.sourceId=01cb7dfca117fbb591360a2b6e46912e \
    -Dapi.test.identitySourceId=01cb7dfca117fbb591360a2a5b64177c \
    -Dapi.test.customerId=C14e3bmn2 \
    -Dapi.test.domain=company.com
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
