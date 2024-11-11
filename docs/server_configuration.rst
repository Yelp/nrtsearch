Server Configuration
==========================
The server configuration is an optional YAML file which is provided to the command line when starting the server.

Usage

.. code-block::

  ./build/install/nrtsearch/bin/nrtsearch_server server_configuration.yaml

Example server configuration

.. code-block:: yaml

  nodeName: "nrtsearch_primary"
  hostName: "primary-node"
  port: "8000"
  replicationPort: "8001"
  stateDir: "/user/app/primary_state"
  indexDir: "/user/app/primary_index_base"
  threadPoolConfiguration:
    search:
      maxThreads: 4
    index:
      maxThreads: 18
  botoCfgPath: "/user/app/boto.cfg"
  bucketName: "nrtsearch-bucket"
  serviceName: "nrtsearch-service-test"


.. list-table:: `NrtsearchConfig <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/config/NrtsearchConfig.java>`_
   :widths: 25 10 50 25
   :header-rows: 1

   * - Property
     - Type
     - Description
     - Default

   * - nodeName
     - str
     - Name of this NrtSearch instance. Currently used for registering replicas with primary. This property supports `environment variable substitution <https://github.com/Yelp/nrtsearch/blob/6a9049a840fc2da4816e2a6cf1837bd31218ae97/src/main/java/com/yelp/nrtsearch/server/config/NrtsearchConfig.java#L386>`_.
     - main

   * - hostName
     - str
     - Hostname of this NrtSearch instance. Replicas use this property when registering with the primary. This property supports `environment variable substitution <https://github.com/Yelp/nrtsearch/blob/6a9049a840fc2da4816e2a6cf1837bd31218ae97/src/main/java/com/yelp/nrtsearch/server/config/NrtsearchConfig.java#L386>`_.
     - localhost

   * - port
     - str
     - Port for NrtsearchServer gRPC requests
     - 50051

   * - replicationPort
     - str
     - Port for ReplicationServer gRPC requests
     - 50052

   * - stateDir
     - str
     - Path of global state directory
     - `<DEFAULT_USER_DIR> <https://github.com/Yelp/nrtsearch/blob/6a9049a840fc2da4816e2a6cf1837bd31218ae97/src/main/java/com/yelp/nrtsearch/server/config/NrtsearchConfig.java#L45>`_/default_state

   * - indexDir
     - str
     - Path of directory containing index state and segments
     - `<DEFAULT_USER_DIR> <https://github.com/Yelp/nrtsearch/blob/6a9049a840fc2da4816e2a6cf1837bd31218ae97/src/main/java/com/yelp/nrtsearch/server/config/NrtsearchConfig.java#L45>`_/default_index

   * - bucketName
     - str
     - Name of bucket to use for external storage
     - DEFAULT_REMOTE_BUCKET

   * - maxS3ClientRetries
     - int
     - Max retries to configure for the server s3 client. If <= 0, the default retry policy is used.
     - 20

   * - botoCfgPath
     - str
     - Path to AWS credentials (if using S3 for remote storage); Will use the DefaultAWSCredentialsProviderChain if omitted.
     - null

   * - deadlineCancellation
     - bool
     - Enables gRPC deadline based cancellation of requests. A request is cancelled early if it exceeds the deadline. Currently only supported by the search endpoint.
     - true

   * - lowPriorityCopyPercentage
     - int
     - Percentage of gRPC data copy cycles to give priority to low priority (merge pre copy) tasks. The remaining cycles give priority to high priority (nrt point) tasks, if present.
     - 0

   * - plugins
     - list
     - List of plugins located in the ``pluginSearchPath`` to load
     - []

   * - pluginSearchPath
     - str or list
     - Search paths for plugins. This can either be a single string or a list of strings. The server will try to find the first directory in the search path containing a given plugin.
     - plugins

   * - useKeepAliveForReplication
     - bool
     - If enabled, the primary will enable keepAlive on the replication channel with keepAliveTime 1 minute and keepAliveTimeout 10 seconds. Replicas ignore this option.
     - true

.. list-table:: `Threadpool Configuration <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/config/ThreadPoolConfiguration.java>`_ (``threadPoolConfiguration.*``)
   :widths: 25 10 50 25
   :header-rows: 1

   * - Property
     - Type
     - Description
     - Default

   * - search.maxThreads
     - int
     - Size of searcher threadpool executor
     - (numCPUs * 3) / 2 + 1

   * - search.maxBufferedItems
     - int
     - Max tasks that can be queued by searcher threadpool executor
     - max(1000, 2 * ((numCPUs * 3) / 2 + 1))

   * - search.threadNamePrefix
     - string
     - Name prefix for threads created by searcher threadpool executor
     - LuceneSearchExecutor

   * - index.maxThreads
     - int
     - Size of indexing threadpool executor
     - numCPUs + 1

   * - index.maxBufferedItems
     - int
     - Max tasks that can be queued by indexing threadpool executor
     - max(200, 2 * (numCPUs + 1))

   * - index.threadNamePrefix
     - string
     - Name prefix for threads created by indexing threadpool executor
     - LuceneIndexingExecutor

   * - server.maxThreads
     - int
     - Size of NrtsearchServer threadpool executor
     - numCPUs + 1

   * - server.maxBufferedItems
     - int
     - Max tasks that can be queued by NrtsearchServer threadpool executor
     - max(200, 2 * (numCPUs + 1))

   * - server.threadNamePrefix
     - string
     - Name prefix for threads created by NrtsearchServer threadpool executor
     - GrpcServerExecutor

   * - replicationserver.maxThreads
     - int
     - Size of ReplicationServer threadpool executor
     - numCPUs + 1

   * - replicationserver.maxBufferedItems
     - int
     - Max tasks that can be queued by ReplicationServer threadpool executor
     - max(200, 2 * (numCPUs + 1))

   * - replicationserver.threadNamePrefix
     - string
     - Name prefix for threads created by ReplicationServer threadpool executor
     - GrpcReplicationServerExecutor

   * - fetch.maxThreads
     - int
     - Size of fetch threadpool executor
     - 1

   * - fetch.maxBufferedItems
     - int
     - Max tasks that can be queued by fetch threadpool executor
     - max(1000, 2 * ((numCPUs * 3) / 2 + 1))

   * - fetch.threadNamePrefix
     - string
     - Name prefix for threads created by fetch threadpool executor
     - LuceneFetchExecutor

   * - grpc.maxThreads
     - int
     - Size of gRPC threadpool executor
     - 2 * numCPUs

   * - grpc.maxBufferedItems
     - int
     - Max tasks that can be queued by gRPC threadpool executor
     - 8

   * - grpc.threadNamePrefix
     - string
     - Name prefix for threads created by gRPC threadpool executor
     - GrpcExecutor

   * - metrics.maxThreads
     - int
     - Size of metrics threadpool executor
     - numCPUs

   * - metrics.maxBufferedItems
     - int
     - Max tasks that can be queued by metrics threadpool executor
     - 8

   * - metrics.threadNamePrefix
     - string
     - Name prefix for threads created by metrics threadpool executor
     - MetricsExecutor

   * - vectormerge.maxThreads
     - int
     - Size of vector merge threadpool executor
     - numCPUs

   * - vectormerge.maxBufferedItems
     - int
     - Max tasks that can be queued by vector merge threadpool executor
     - max(100, 2 * numCPUs)

   * - vectormerge.threadNamePrefix
     - string
     - Name prefix for threads created by vector merge threadpool executor
     - VectorMergeExecutor

.. list-table:: `Alternative Max Threads Config <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/config/ThreadPoolConfiguration.java>`_ (``threadPoolConfiguration.*.maxThreads.*``)
   :widths: 25 10 50 25
   :header-rows: 1

   * - Property
     - Type
     - Description
     - Default

   * - min
     - int
     - Minimum number of threads
     - 1

   * - max
     - int
     - Maximum number of threads
     - INT_MAX

   * - multiplier
     - float
     - Multiplier in threads formula: (numCPUs * multiplier) + offset
     - 1.0

   * - offset
     - int
     - Offset in threads formula: (numCPUs * multiplier) + offset
     - 0

.. list-table:: `Warmer Configuration <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/warming/WarmerConfig.java>`_ (``warmer.*``)
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

.. list-table:: `Index Data Preload Configuration <https://github.com/Yelp/nrtsearch/blob/main/src/main/java/com/yelp/nrtsearch/server/config/IndexPreloadConfig.java>`_ (``preload.*``)
   :widths: 25 10 50 25
   :header-rows: 1

   * - Property
     - Type
     - Description
     - Default

   * - enabled
     - bool
     - If opening index files with an MMapDirectory should preload the data into the OS page cache
     - false

   * - extensions
     - list
     - List of index file extensions to preload. Including '*' will preload all files.
     - ['*']