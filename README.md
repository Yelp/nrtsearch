# nrtSearch
A high performance gRPC server, with optional REST APIs on top of [Apache Lucene](http://lucene.apache.org/) version 8.x source, exposing lucene's
core functionality over a simple gRPC based API.

# Features
* Relies on Lucene's [near-real-time segment replication](http://blog.mikemccandless.com/2017/09/lucenes-near-real-time-segment-index.html) for data replication. This means, a dedicated primary/writer node takes care of indexing operations and expensive operations like [segment merges](http://blog.mikemccandless.com/2011/02/visualizing-lucenes-segment-merges.html). This allows the replicas' system resources to be dedicated entirely for search queries. This behavior is in contrast to the document replication approach taken by some other popular seach engines based on lucene like elasticsearch where every node is a writer and a reader.
* [Supports concurrent query execution](http://blog.mikemccandless.com/2019/10/concurrent-query-execution-in-apache.html). This is another feature missing from popular search engines based on lucene like elasticsearch.
* Can be deployed as a "stateless microservice". Indexes are backed up in s3. Clients can choose to commit data outside of this system once their backup is complete. Upon restarts e.g if you bring up a new container clients can choose to bootstrap indexes from their previous backed up state. Ability to deploy in a stateless manner allows for easy scalibility using container tools like kubernetes, mesos etc.
* Provides gRPC streaming APIs for indexing and searching. Also supports REST APIs.

# Design
The design goals are mostly similar to the ones mentioned in the [Lucene Server](https://github.com/mikemccand/luceneserver#design) project. This project uses ideads and code from luceneserver and builds on them.

A single node can index a *stream* of documents, run near-real-time searches via a parsed query string, including "scrolled" searches, sorting, index-time sorting, etc.

Fields must first be registered with the *registerFields* command, where you express whether you will search, sort etc., and then documents can be indexed with those fields.

There is no transaction log, so you must call *commit* yourself periodically to make recent changes durable on disk. This means that if a node crashes, all indexed documents since the last commit are lost.

# Indexing a stream of documents
nrtSearch supports client side gRPC streaming for its *addDocuments* endpoint. This means that the server API accepts a stream of documents . The client can choose to stream the documents however it wishes.
The example nrtSearch client implemented here reads a CSV file and streams documents from it over to the server. The server can index chunks of documents the size of which is configurable as the client
continues to send more documents over its stream. gRPC enables this with minimal application code and yields higher performance compared to JSON. TODO[citation needed]: Add performance numbers of stream based indexing for some datasets.

# Near-real-time-replication
This requirement is one of the primary reasons to create this project. [near-real-time-replication](https://issues.apache.org/jira/browse/LUCENE-5438) seems a good alternative to document based replication when it comes to costs associated with maintaing large clusters. Scaling document based clusters up/down in a timely manner could be slower
due to data migration between nodes apart from paying the cost for reindexing on all nodes.

Below is a depiction of how the system works in regards to Near-real-time(NRT) replication and durability.
![alt text](https://github.com/Yelp/platypus/blob/master/src/images/nrt.png "Platypus NRT and durability")

* Primary node comes up with either no index or can restore an index from remote storage if the `restore` option is specified by the client on the `startIndex` command. This node will accept `indexing` requests from clients. It will also periodically  `publishNrtUpdate` to replicas giving them a chance to catch up with latest primary indexing changes.
* Replica nodes are also started using the `startIndex` command. They will sync with the current primary and update their indexes using lucene's NRT APIs. These nodes will serve client's `search` queries.
* Each time client invokes `commit` on primary, it will save its current index state and related metadata e.g. schemas, settings to a remote storage. Clients should use the ack from this endpoint to commit the data in their channel e.g. kafka.
* If a replica crashes, a new one can be brought up and will re-sync with the current primary. It will register itself with the primary once its brought up.
* If a primary crashes, a new one can be brought up with the `restore` option on `startIndex` command to regain previous stored state. The replicas will then re-sync their indexes with the primary.


# Build Server and Client
In the home directory.

```
./gradlew clean && ./gradlew installDist && ./gradlew test
```

Note: This code has been tested on *Java14*


# Run gRPC Server

```
./build/install/nrtsearch/bin/lucene-server
```

# Run REST Server (use the appropriate binary for your platform e.g. for mac os)

```
./build/install/nrtsearch/bin/http_wrapper-darwin-amd64 <gRPC_PORT> <REST_PORT>
```

# Example to run some basic client commands
## Create Index

```
./build/install/nrtsearch/bin/lucene-client createIndex --indexName  testIdx --rootDir testIdx
```

```
curl -XPOST localhost:<REST_PORT>/v1/create_index -d '{"indexName": "testIdx", "rootDir": "testIdx"}'
```

## Update Settings

```
./build/install/nrtsearch/bin/lucene-client settings -f settings.json
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
./build/install/nrtsearch/bin/lucene-client startIndex -f startIndex.json
cat startIndex.json
{
  "indexName" : "testIdx"
}
```

## RegisterFields

```
./build/install/nrtsearch/bin/lucene-client registerFields -f registerFields.json
cat registerFields.json
{             "indexName": "testIdx",
              "field":
              [
                      { "name": "doc_id", "type": "ATOM", "storeDocValues": true},
                      { "name": "vendor_name", "type": "TEXT" , "search": true, "store": true, "tokenize": true},
                      { "name": "license_no",  "type": "INT", "multiValued": true, "storeDocValues": true}
              ]
}
```

## Add Documents

```
./build/install/nrtsearch/bin/lucene-client addDocuments -i testIdx -f docs.csv -t csv
cat docs.csv
doc_id,vendor_name,license_no
0,first vendor,100;200
1,second vendor,111;222
```

## Search

```
./build/install/nrtsearch/bin/lucene-client search -f search.json
cat search.json
{
        "indexName": "testIdx",
        "startHit": 0,
        "topHits": 100,
        "retrieveFields": ["doc_id", license_no", "vendor_name"],
         "queryText": "vendor_name:first vendor"
}
```


# API documentation
The build uses protoc-gen-doc program to generate the documentation needed in html (or markdown) files from proto files. It is run inside a docker container. The gradle task to generate this documentation is as follows.

```
./gradlew buildDocs
```

This should create a src/main/docs/index.html file that can be seen in your local browser. A [sample snapshot](https://gist.github.com/umeshdangat/468fcf6a8e73f0bd45e197c33a3c2c12#file-platypus_api-png)

# Yelp Indexing tool

# Reviews

This tool indexes yelp reviews available at [Yelp dataset challenge](https://www.yelp.com/dataset/challenge). It runs a default version with only 1k reviews of the `reviews.json` or you could download the yelp dataset and place the review.json in the user.home dir and the tool will use that instead. The complete review.json should have close to 7Million reviews. The tool runs multi-threaded indexing and a search thread in parallel reporting the `totalHits`.  Command to run this specific test:

```
./gradlew clean && ./gradlew installDist && ./gradlew :test -PincludePerfTests=* --tests "com.yelp.nrtsearch.server.YelpReviewsTest.runYelpReviews" --info
```

# Suggestions

This test indexes businesses, creates an Infix Suggester and fetches suggestions. It requires a host, a port and a writeable directory in a standalone nrtSearch server.

```./gradlew :test -DsuggestTmp=remoteServerDir -DsuggestHost=yourStandaloneServerHost -DsuggestPort=yourStandaloneServerHost --tests "com.yelp.nrtsearch.server.YelpSuggestTest"```
