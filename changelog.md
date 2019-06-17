# Connector release changelog

## v1-0.0.5 changelist
### SDK
* Avoid inheritance when DefaultAcl is configured to be public
* Catch errors and runtime exceptions in traverser workers
* Do not decode + to space in resource names.
* Add IndexingItemBuilder support for searchQualityMetadata.quality config
* Parameter: itemMetadata.objectType is deprecated, use itemMetadata.objectType.defaultValue=Item

### Sharepoint
* Fix indexing when the URL contains special characters

### Norconex

* Support multiple crawlers
* Handle empty content files

### Nutch

* Upgraded to Nutch 1.15
* Filename changes
  * indexer-google-cloud-search-XXX.zip to google-cloudsearch-apache-nutch-indexer-plugin-XXX.zip
  * The plug-in directory (inside the ZIP file) changed from plugins/indexer-google-cloud-search to plugins/indexer-google-cloudsearch (the "-" is removed)
* Settings for crawler should use ‘index-writers.xml’ file

## v1-0.0.4 changelist

### Identity SDK update

* Upgrade Cloud Identity client to use V1 API

### Indexing SDK changes

* Added a new configuration IndexItemOptions to specify if the index request should allow G Suite principals that do not exist or are deleted (API change)
* Fixed unit test failures due to timezone changes
* Add options to proxy authentication: HTTP or SOCKS
* Optimize CPU usage by simplifying execution logic on a thread pool

### CSV connecter changes

* CSV - Support different CSV Formats
