# Platypus
A high performance gRPC server on top of [Apache Lucene](http://lucene.apache.org/) version 8.x source, exposing lucene's
core functionality over a simple gRPC based API.

Please note that 
* this code is heavily influenced by another github project [Lucene Server](https://github.com/mikemccand/luceneserver)
since it aspires to use some of the similar design principles like [near-realtime-replication](https://github.com/mikemccand/luceneserver#near-real-time-replication).
* it is a work in progress with several features missing which are compiled in the [TODO](https://github.com/umeshdangat/platypus/blob/master/TODO)

# Design
The design goals are mostly similar to the ones mentioned in the [Lucene Server](https://github.com/mikemccand/luceneserver#design) project from where the source code is based off. 

A single node can index a *stream* of documents, run near-real-time searches via a parsed query string, including "scrolled" searches, sorting, index-time sorting, etc.

Fields must first be registered with the *registerFields* command, where you express whether you will search, sort etc., and then documents can be indexed with those fields.

There is no transaction log, so you must call *commit* yourself periodically to make recent changes durable on disk. This means that if a node crashes, all indexed documents since the last commit are lost.

# Indexing a stream of documents
platypus supports client side gRPC streaming for its *addDocuments* endpoint. This means that the server API accepts a stream of documents . The client can choose to stream the documents however it wishes.
The example platypus client implemented here reads a CSV file and streams documents from it over to the server. The server can index chunks of documents the size of which is configurable as the client
continues to send more documents over its stream. gRPC enables this with minimal application code and yields higher performance compared to JSON. TODO[citation needed]: Add performance numbers of stream based indexing for some datasets.

# Near-real-time-replication
This requirement is one of the primary reasons to create this project. [near-real-time-replication](https://issues.apache.org/jira/browse/LUCENE-5438) seems a good alternative to document based replication when it comes to costs associated with 
maintaing large clusters. Scaling document based clusters up/down in a timely manner could be slower due to data migration between nodes apart
from payig the cost for reindexing as you add more replicas.

# Build Server and Client
In the home directory.

```
./gradlew clean && ./gradlew installDist
```

Note: This code has been tested on *Java12*

# Run Server

```
./build/install/platypus/bin/lucene-server
```

# Example to run some basic client commands
## Create Index

```
./build/install/platypus/bin/lucene-client createIndex --indexName  testIdx --rootDir testIdx
```

## Update Settings

```
./build/install/platypus/bin/lucene-client settings -f settings.json
cat settings.json
{             "indexName": "testIdx",
              "indexVerbose": false,
              "directory": "MMapDirectory",
              "nrtCachingDirectoryMaxSizeMB": 0.0,
              "indexMergeSchedulerAutoThrottle": false,
              "concurrentMergeSchedulerMaxMergeCount": 16,
              "concurrentMergeSchedulerMaxThreadCount": 8
}
```

## Start Index

```
./build/install/platypus/bin/lucene-client startIndex -f startIndex.json
cat startIndex.json
{
  "indexName" : "testIdx"
}
```

## RegisterFields

```
./build/install/platypus/bin/lucene-client registerFields -f registerFields.json
cat registerFields.json
{             "indexName": "testIdx",
              "field":
              [
                      { "name": "vendor_name", "type": "TEXT" , "search": true, "store": true, "tokenize": true},
                      { "name": "license_no",  "type": "INT", "multiValued": true, "storeDocValues": true}
              ]
}
```

## Add Documents

```
./build/install/platypus/bin/lucene-client addDocuments -i testIdx -f docs.csv
cat docs.csv
doc_id,vendor_name,license_no
0,first vendor,100;200
1,second vendor,111;222
```

## Search

```
./build/install/platypus/bin/lucene-client search -f search.json
cat search.json
{
        "indexName": "testIdx",
        "startHit": 0,
        "topHits": 100,
        "retrieveFields": ["license_no", "vendor_name"],
         "queryText": "vendor_name:first vendor"
}
```


# API documentation
The build uses protoc-gen-doc program to generate the documentation needed in html (or markdown) files from proto files. It is run inside a docker container. The gradle task to generate this documentation is as follows.

```
./gradlew buildDocs
```

This should create a src/main/docs/index.html file that can be seen in your local browser. A [sample snapshot](https://gist.github.com/umeshdangat/468fcf6a8e73f0bd45e197c33a3c2c12#file-platypus_api-png)

