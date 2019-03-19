# Connector release changelog

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
