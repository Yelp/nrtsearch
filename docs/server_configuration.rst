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
     - Hostname of this NrtSearch instance. Replicas use this property when registering with the primary. This property supports `environment variable substitution <https://github.com/Yelp/nrtsearch/blob/2ae8bae079ae8a8a59bb896fee775919235710aa/src/main/java/com/yelp/nrtsearch/server/config/LuceneServerConfiguration.java#L298>`_.
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
     - `<DEFAULT_USER_DIR> <https://github.com/Yelp/nrtsearch/blob/f612f5d3e14e468ab8c9b45dd4be0ab84231b9de/src/main/java/com/yelp/nrtsearch/server/config/LuceneServerConfiguration.java#L35>`_/default_state

   * - indexDir
     - str
     - Path of directory containing index state and segments
     - `<DEFAULT_USER_DIR> <https://github.com/Yelp/nrtsearch/blob/f612f5d3e14e468ab8c9b45dd4be0ab84231b9de/src/main/java/com/yelp/nrtsearch/server/config/LuceneServerConfiguration.java#L35>`_/default_index

   * - bucketName
     - str
     - Name of bucket to use for external storage
     - DEFAULT_ARCHIVE_BUCKET

   * - botoCfgPath
     - str
     - Path to AWS credentials (if using S3 for remote storage)
     - `<DEFAULT_USER_DIR> <https://github.com/Yelp/nrtsearch/blob/f612f5d3e14e468ab8c9b45dd4be0ab84231b9de/src/main/java/com/yelp/nrtsearch/server/config/LuceneServerConfiguration.java#L35>`_/boto.cfg

   * - archiveDirectory
     - str
     - Directory for uploading/downloading from external storage. 
     - `<DEFAULT_USER_DIR> <https://github.com/Yelp/nrtsearch/blob/f612f5d3e14e468ab8c9b45dd4be0ab84231b9de/src/main/java/com/yelp/nrtsearch/server/config/LuceneServerConfiguration.java#L35>`_/archiver

   * - downloadAsStream
     - bool
     - If enabled, the content downloader will perform a streaming extraction of tar archives from remote storage to disk. Otherwise, the downloader will only extract after finishing downloading the archive to disk.
     - true

   * - restoreState
     - bool
     - Enables loading state from external storage on startup
     - false

   * - restoreFromIncArchiver
     - bool
     - If enabled, uses the incremental archiver when restoring index data and state
     - false

   * - backupWithIncArchiver
     - bool
     - If enabled, uses the incremental archiver for backups
     - false

   * - deadlineCancellation
     - bool
     - Enables gRPC deadline based cancellation of requests. A request is cancelled early if it exceeds the deadline. Currently only supported by the search endpoint.
     - false

   * - plugins
     - list
     - List of plugins located in the ``pluginSearchPath`` to load
     - []

   * - pluginSearchPath
     - str
     - Search paths for plugins. These paths are separated by the system path separator character (; on Windows, : on Mac and Unix). The server will try to find the first directory in the search path matching a given plugin. 
     - plugins

   * - publishJvmMetrics
     - bool
     - If enabled, registers JVM metrics with prometheus. 
     - true

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
     - Maximum number of queries to store for warming
     - 0

   * - warmingParallelism
     - int
     - Parallelism of queries during warming
     - 1

   * - warmOnStartup
     - bool
     - Whether the server should warm on startup
     - false

.. list-table:: `State Configuration <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/config/StateConfig.java>`_ (``stateConfig.*``)
   :widths: 25 10 50 25
   :header-rows: 1

   * - Property
     - Type
     - Description
     - Default

   * - backendType
     - enum
     - Chooses which backend to use for storing and loading state. ``LOCAL`` uses the local disk as the source of truth for global and index state. ``REMOTE`` uses external storage as the source of truth for global and index state.
     - ``LOCAL``

.. list-table:: `File Copy Configuration <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/config/FileCopyConfig.java>`_ (``FileCopyConfig.*``)
   :widths: 25 10 50 25
   :header-rows: 1

   * - Property
     - Type
     - Description
     - Default

   * - ackedCopy
     - bool
     - If enabled, replicas use acked file copy when copying files from the primary.
     - false

   * - chunkSize
     - int
     - Size of chunks when the primary sends files to replicas.
     - 64 * 1024

   * - ackEvery
     - int
     - Number of chunks sent to a replica between acks.
     - 1000

   * - maxInFlight
     - int
     - Maximum number of in-flight chunks sent by the primary.
     - 2000