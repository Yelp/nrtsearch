Server Configuration
==========================
The server configuration is an optional YAML file which is provided to the command line when starting the server.

Usage

.. code-block::

  ./build/install/nrtsearch/bin/lucene-server server_configuration.yaml

Example server configuration

.. code-block:: yaml

  nodeName: "lucene_server_primary"
  hostName: "primary-node"
  port: "8000"
  replicationPort: "8001"
  stateDir: "/user/app/primary_state"
  indexDir: "/user/app/primary_index_base"
  threadPoolConfiguration:
    maxSearchingThreads: 4
    maxIndexingThreads: 18
  fileSendDelay: false
  botoCfgPath: "/user/app/boto.cfg"
  bucketName: "nrtsearch-bucket"
  archiveDirectory: "/user/app/primary_index_archiver"
  serviceName: "nrtsearch-service-test"
  restoreState: False
  restoreFromIncArchiver: "true"
  backupWithIncArchiver: "true"
  downloadAsStream: "true"


.. list-table:: `LuceneServerConfiguration <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/config/LuceneServerConfiguration.java>`_
   :widths: 25 10 50 25
   :header-rows: 1

   * - Property
     - Type
     - Description
     - Default

   * - nodeName
     - str
     - Name of this NrtSearch instance. Currently used for emitting metrics labels.
     - main

   * - hostName
     - str
     - Hostname of this NrtSearch instance
     - localhost

   * - port
     - str
     - Port for LuceneServer gRPC requests
     - 50051

   * - replicationPort
     - str
     - Port for ReplicationServer gRPC requests
     - 50052

   * - stateDir
     - str
     - Path of global state directory
     - default_state

   * - indexDir
     - str
     - Path of directory containing index state and segments
     - default_index

   * - bucketName
     - str
     - Name of bucket to use for external storage
     - DEFAULT_ARCHIVE_BUCKET

   * - botoCfgPath
     - str
     - Path to AWS credentials (if using S3 for remote storage)
     - boto.cfg

   * - archiveDirectory
     - str
     - Directory for uploading/downloading from external storage
     - archiver

   * - downloadAsStream
     - bool
     - Whether the downloader should stream instead of writing to a file first
     - true

   * - restoreState
     - bool
     - Enables loading state from external storage on startup
     - false

   * - restoreFromIncArchiver
     - bool
     - If enabled, uses the incremental archiver when restoring state
     - false

   * - backupWithIncArchiver
     - bool
     - If enabled, uses the incremental archiver for backups
     - false

   * - deadlineCancellation
     - bool
     - Enables gRPC deadline based cancellation of requests
     - false

   * - plugins
     - list
     - List of plugins located in the ``pluginSearchPath`` to load
     - []

   * - pluginSearchPath
     - str
     - Search path for plugins. The server will try to find the first directory in the search path matching a given plugin.
     - plugins

.. list-table:: `Threadpool Configuration <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/config/ThreadPoolConfiguration.java>`_ (``threadPoolConfiguration.*``)
   :widths: 25 10 50 25
   :header-rows: 1

   * - Property
     - Type
     - Description
     - Default

   * - maxSearchingThreads
     - int
     - Size of searcher threadpool executor
     - (numCPUs * 3) / 2 + 1

   * - maxFetchThreads
     - int
     - Size of fetch threadpool executor
     - 1

   * - maxIndexingThreads
     - int
     - Size of indexing threadpool executor
     - numCPUs + 1

   * - maxGrpcLuceneserverThreads
     - int
     - Size of LuceneServer threadpool executor
     - numCPUs + 1

   * - maxGrpcReplicationserverThreads
     - int
     - Size of ReplicationServer threadpool executor
     - numCPUs + 1

.. list-table:: `Warmer Configuration <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/luceneserver/warming/WarmerConfig.java>`_ (``warmer.*``)
   :widths: 25 10 50 25
   :header-rows: 1

   * - Property
     - Type
     - Description
     - Default

   * - maxWarmingQueries
     - int
     - Maximum number of queries to send during warming
     - 0

   * - warmingParallelism
     - int
     - Parallelism of queries during warming
     - 1

   * - warmOnStartup
     - bool
     - Whether the server should warm on startup
     - false
