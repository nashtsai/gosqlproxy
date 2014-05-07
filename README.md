# Go SQL proxy driver

Go SQL proxy driver aimed for heavy write performance

Status: planning

## Goal

* Provide Go SQL proxy driver to handle multiple data sources, primarily for master-slave, master-master replication RMDBS (i.e., MySQL, PostgreSQL)
* Slave DB will be choosen by default for SELECT operation
* Async DB INSERT/UPDATE/DELETE operations for maximium write IO throughput
