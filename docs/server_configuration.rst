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
      useVirtualThreads: true
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

   * - enableGlobalBucketAccess
     - bool
     - If enabled, the S3 client uses cross-region access, allowing it to access buckets in any region without requiring an exact region match.
     - false

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

   * - maxClauseCount
     - int
     - Maximum number of clauses in a query
     - 1024
   
   * - useSeparateCommitExecutor
     - bool
     - If enabled, the server will use a separate executor for commit operations instead of using the indexing executor.
     - false

   * - requireIdField
     - bool
     - If enabled, all indices must contain an _ID field to be started.
     - false

   * - embeddingProviders
     - map
     - Named embedding provider configurations for text-to-vector conversion. Each entry maps a provider name to its config (must include ``type`` and type-specific settings). See :ref:`text-based-knn-search` in :doc:`vector_search` for details.
     - {}

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

   * - search.useVirtualThreads
     - bool
     - Whether to use virtual threads instead of a traditional thread pool for search operations
     - false

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

   * - index.useVirtualThreads
     - bool
     - Whether to use virtual threads instead of a traditional thread pool for indexing operations
     - false

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

   * - server.useVirtualThreads
     - bool
     - Whether to use virtual threads instead of a traditional thread pool for server operations
     - false

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

   * - replicationserver.useVirtualThreads
     - bool
     - Whether to use virtual threads instead of a traditional thread pool for replication server operations
     - false

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

   * - fetch.useVirtualThreads
     - bool
     - Whether to use virtual threads instead of a traditional thread pool for fetch operations
     - false

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

   * - grpc.useVirtualThreads
     - bool
     - Whether to use virtual threads instead of a traditional thread pool for gRPC operations
     - false

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

   * - metrics.useVirtualThreads
     - bool
     - Whether to use virtual threads instead of a traditional thread pool for metrics operations
     - false

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

   * - vectormerge.useVirtualThreads
     - bool
     - Whether to use virtual threads instead of a traditional thread pool for vector merge operations
     - false

   * - commit.maxThreads
     - int
     - Size of commit threadpool executor
     - 5

   * - commit.maxBufferedItems
     - int
     - Max tasks that can be queued by commit threadpool executor
     - 5

   * - commit.threadNamePrefix
     - string
     - Name prefix for threads created by commit threadpool executor
     - CommitExecutor

   * - commit.useVirtualThreads
     - bool
     - Whether to use virtual threads instead of a traditional thread pool for commit operations
     - false

   * - remote.maxThreads
     - int
     - Size of remote threadpool executor
     - 20

   * - remote.maxBufferedItems
     - int
     - Max tasks that can be queued by remote threadpool executor
     - INT_MAX

   * - remote.threadNamePrefix
     - string
     - Name prefix for threads created by remote threadpool executor
     - RemoteExecutor

   * - remote.useVirtualThreads
     - bool
     - Whether to use virtual threads instead of a traditional thread pool for remote operations
     - false

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
     - If enabled, replicas use acked file copy (bidirectional streaming with flow control) when copying files from the primary. Without this, the primary streams all chunks in a tight loop with no backpressure, which can cause unbounded gRPC buffer growth and memory pressure when replicas are slower than the primary, index segments are large, or many replicas replicate simultaneously. When enabled, the primary pauses after sending ``maxInFlight`` un-acked chunks and resumes when the replica acknowledges receipt. The ``chunkSize``, ``ackEvery``, and ``maxInFlight`` settings below apply only when this is enabled.
     - false

   * - chunkSize
     - int
     - Size in bytes of each chunk the primary sends to replicas during file copy. Only used when ``ackedCopy`` is true.
     - 64 * 1024

   * - ackEvery
     - int
     - Number of chunks sent to a replica between acks. Must be less than or equal to ``maxInFlight``. Only used when ``ackedCopy`` is true.
     - 1000

   * - maxInFlight
     - int
     - Maximum number of un-acked chunks the primary is allowed to have in flight. The primary pauses sending when this limit is reached and resumes upon receiving an ack. Only used when ``ackedCopy`` is true.
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

.. list-table:: `S3 Remote Storage Configuration <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/remote/s3/S3Util.java>`_ (``remoteConfig.s3.*``)
   :widths: 25 10 50 25
   :header-rows: 1

   * - Property
     - Type
     - Description
     - Default

   * - asyncClientType
     - str
     - Type of async S3 client to use for index file transfers. ``java`` uses the Netty-based ``S3AsyncClient`` (recommended; supports ``MetricPublisher`` and full connection pool configuration). ``crt`` uses the AWS C Runtime ``S3CrtAsyncClient`` (higher throughput but limited observability).
     - java

   * - metrics
     - bool
     - If enabled, tracks S3 download byte counts per index via the ``nrt_s3_download_bytes_total`` Prometheus metric.
     - false

   * - rateLimitPerSecond
     - str
     - Maximum download throughput for S3 operations. Accepts a plain byte count or a size string with a suffix (e.g. ``500mb``, ``1gb``). Set to ``0`` to disable rate limiting.
     - 0 (unlimited)

   * - rateLimitWindowSeconds
     - int
     - Window duration in seconds for the download rate limiter. Must be > 0.
     - 1

   * - downloadBatchSize
     - int
     - Maximum number of index files downloaded concurrently in a single batch during bootstrap. When set to ``0``, the server's ``defaultParallelism`` value is used.
     - 0

.. list-table:: `S3 Java Async Client Configuration <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/remote/s3/S3Util.java>`_ (``remoteConfig.s3.java.*``)
   :widths: 25 10 50 25
   :header-rows: 1

   * - Property
     - Type
     - Description
     - Default

   * - minimumPartSize
     - str
     - Minimum size of each part in a multipart upload or download. Accepts a plain byte count or a size string with a suffix (e.g. ``16mb``).
     - 8mb

   * - thresholdSize
     - str
     - File size threshold above which multipart upload is used. Accepts a plain byte count or a size string with a suffix.
     - 8mb

   * - apiCallBufferSize
     - str
     - Buffer size for API calls in bytes. Accepts a plain byte count or a size string with a suffix. Set to ``0`` to use the SDK default.
     - 0 (SDK default)

   * - maxInFlightParts
     - int
     - Maximum number of concurrent in-flight multipart parts during a transfer. Set to ``0`` to use the SDK default.
     - 0 (SDK default)

   * - ioThreads
     - int
     - Number of Netty NIO event loop threads. Each thread can multiplex many connections. Set to ``0`` to use the SDK default (``2 * numCPUs``).
     - 0 (SDK default)

   * - maxConnections
     - int
     - Maximum number of concurrent HTTP connections in the Netty connection pool. Set to ``0`` to use the SDK default of 50.
     - 100

   * - connectionTimeoutMs
     - int
     - TCP connection establishment timeout in milliseconds. Set to ``0`` to use the SDK default of 2000ms.
     - 0 (SDK default)

   * - connectionAcquisitionTimeoutMs
     - int
     - Maximum time in milliseconds to wait for a connection from the pool before failing. Increase this or ``maxConnections`` when seeing pool exhaustion errors during high-concurrency bootstrap. Set to ``0`` to use the SDK default of 10000ms.
     - 60000

   * - maxPendingConnectionAcquires
     - int
     - Maximum number of requests that can be queued waiting for a connection from the pool. Set to ``0`` to use the SDK default of 10000.
     - 0 (SDK default)

.. list-table:: `S3 CRT Async Client Configuration <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/remote/s3/S3Util.java>`_ (``remoteConfig.s3.crt.*``)
   :widths: 25 10 50 25
   :header-rows: 1

   * - Property
     - Type
     - Description
     - Default

   * - minimumPartSize
     - str
     - Minimum size of each part in a multipart transfer. Accepts a plain byte count or a size string with a suffix (e.g. ``16mb``).
     - 8mb

   * - targetThroughputInGbps
     - float
     - Target throughput in Gbps. The CRT client uses this to auto-size its connection count and buffers. Mutually exclusive with ``maxConcurrency``; set to ``0`` to use the SDK default.
     - 0 (SDK default)

   * - maxConcurrency
     - int
     - Hard cap on the number of concurrent S3 connections. Mutually exclusive with ``targetThroughputInGbps``; set to ``0`` to use the SDK default.
     - 0 (SDK default)

   * - maxNativeMemoryLimit
     - str
     - Hard cap on the amount of native (off-heap) memory the CRT client may use. Accepts a plain byte count or a size string with a suffix. Set to ``0`` for unlimited.
     - 0 (unlimited)

