# Connector release changelog

## v1-0.0.4 changelist
### Identity update
* Upgrade cloud identity client to use V1 API

### Indexing changes
* Add a new configuration IndexItemOptions to specify if the index request should allow gsuite principals that do not exist or are deleted (API change)
* Build failures due to timezone changes - Preserve time zones when parsing dates without times

### Improvements & Bugfixes
* SDK
  * Add options to proxy authentication: HTTP or SOCKS
  * Optimize CPU usage by simplifying execution logic on a thread pool
* CSV - Support different CSV Formats
