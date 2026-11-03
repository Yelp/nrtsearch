Vector Gateway (S3 Vectors compatible API)
==========================================

.. note::

   This is a prototype.

The vector gateway is an HTTP/JSON service that offers an API modeled on
`Amazon S3 Vectors <https://aws.amazon.com/s3/features/vectors/>`_, with an nrtsearch primary
behind it. It lets you run a "vector bucket" style service on nrtsearch. Index data is kept in S3
by nrtsearch's remote backend, and indexes that are not used are stopped so they cost little more
than S3 storage.

Running
-------

The gateway needs an nrtsearch server running in ``PRIMARY`` mode. For idle index tiering, the
server must use ``indexStartConfig.dataLocationType: REMOTE`` so that a stopped index can be
restored from S3.

The distribution includes a ``vector_gateway`` script in ``bin/``, next to ``nrtsearch_server`` and
``nrt_utils``. Build it with ``./gradlew installDist``, then run:

.. code-block:: bash

  ./build/install/nrtsearch/bin/vector_gateway \
    --port 8080 \
    --nrtsearchHost localhost --nrtsearchPort 50051 \
    --registryFile /var/lib/vector_gateway/registry.json \
    --idleStopSeconds 900

Run ``vector_gateway --help`` to see all options.

.. list-table::
   :header-rows: 1

   * - Option
     - Default
     - Description
   * - ``--port``
     - ``8080``
     - HTTP port to listen on.
   * - ``--nrtsearchHost``, ``--nrtsearchPort``
     - ``localhost``, ``50051``
     - Address of the nrtsearch primary.
   * - ``--registryFile``
     - unset
     - JSON file that stores buckets, index configs and metadata schemas. If unset, they are kept
       only in memory and lost when the gateway stops.
   * - ``--idleStopSeconds``
     - ``0``
     - Stop an index after this many seconds without requests. ``0`` disables tiering.
   * - ``--idleCheckSeconds``
     - ``10``
     - How often to look for idle indexes.
   * - ``--quantizedBits``
     - ``7``
     - Scalar quantization for new indexes: ``1``, ``2``, ``4``, ``7`` or ``8``, or ``0`` for
       full precision HNSW.
   * - ``--rescoreOversample``
     - ``3.0``
     - Rescore this many times ``topK`` hits with full precision vectors when quantized. ``0``
       disables rescoring.
   * - ``--minNumCandidates``
     - ``100``
     - Smallest number of HNSW candidates per query.
   * - ``--threads``
     - ``32``
     - Number of HTTP request threads.

API
---

Each operation is ``POST /<Operation>`` with a JSON body. Request and response fields follow S3
Vectors (camelCase). Errors return an HTTP error status and
``{"__type": "<ErrorCode>", "message": "..."}``.

.. list-table::
   :header-rows: 1

   * - Operation
     - Notes
   * - ``CreateVectorBucket``, ``GetVectorBucket``, ``ListVectorBuckets``, ``DeleteVectorBucket``
     - A bucket must be empty before it can be deleted.
   * - ``CreateIndex``, ``GetIndex``, ``ListIndexes``, ``DeleteIndex``
     - ``dataType`` must be ``float32``. ``distanceMetric`` is ``cosine`` or ``euclidean``.
       ``metadataConfiguration.nonFilterableMetadataKeys`` takes up to 10 keys.
   * - ``PutVectors``
     - Up to 500 vectors. Putting an existing key replaces the vector and all its metadata.
   * - ``GetVectors``
     - Up to 100 keys. ``returnData`` and ``returnMetadata`` are optional.
   * - ``DeleteVectors``
     - Up to 500 keys.
   * - ``ListVectors``
     - Returns vectors in key order, ``maxResults`` up to 1000 per page, paged with
       ``nextToken``. ``segmentCount`` and ``segmentIndex`` are not supported yet.
   * - ``QueryVectors``
     - ``topK`` up to 10,000. ``filter``, ``returnDistance`` and ``returnMetadata`` are optional.

Example:

.. code-block:: bash

  curl -XPOST localhost:8080/CreateVectorBucket -d '{"vectorBucketName": "media"}'
  curl -XPOST localhost:8080/CreateIndex -d '{
    "vectorBucketName": "media", "indexName": "movies", "dataType": "float32",
    "dimension": 4, "distanceMetric": "cosine",
    "metadataConfiguration": {"nonFilterableMetadataKeys": ["synopsis"]}}'
  curl -XPOST localhost:8080/PutVectors -d '{
    "vectorBucketName": "media", "indexName": "movies",
    "vectors": [{"key": "m1", "data": {"float32": [0.1, 0.2, 0.3, 0.4]},
                 "metadata": {"genre": "drama", "year": 2021, "synopsis": "..."}}]}'
  curl -XPOST localhost:8080/QueryVectors -d '{
    "vectorBucketName": "media", "indexName": "movies", "topK": 5,
    "queryVector": {"float32": [0.1, 0.2, 0.3, 0.4]},
    "filter": {"$and": [{"genre": "drama"}, {"year": {"$gte": 2020}}]},
    "returnDistance": true, "returnMetadata": true}'

Metadata filters
----------------

Supported operators: ``$eq``, ``$ne``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, ``$in``, ``$nin``,
``$exists``, ``$startsWith``, ``$and`` and ``$or``.

* A bare value means ``$eq``.
* Several keys in one object, or several operators on one key, are combined with AND.
* For list values, ``$eq`` and ``$in`` match if any element matches.
* ``$ne`` and ``$nin`` also match vectors that don't have the key.
* A filter can have at most 100 constraints. Each value counts as one, so an ``$in`` with 3 values
  counts as 3.

Filters are applied before the vector search (nrtsearch's ``ACORN`` filtered HNSW search, which
falls back to an exact search when the filter is very selective). This is the behavior of the S3
Vectors ``ENHANCED`` index mode: a query returns ``topK`` results whenever at least ``topK``
vectors match the filter.

How data is stored
------------------

Each vector index is an nrtsearch index named ``vb_<bucket>__<index>``, with these fields:

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Use
   * - ``_key``
     - ``_ID``
     - Vector key, used for upserts and deletes.
   * - ``_sort_key``
     - ``ATOM`` (doc values)
     - Sort order and paging for ``ListVectors``.
   * - ``_vector``
     - ``VECTOR``
     - HNSW with scalar quantization (``--quantizedBits``, 7 by default). Query results are
       rescored with the full precision vectors (``rescore_vector``, ``--rescoreOversample``).
   * - ``_meta``
     - ``ATOM`` (stored)
     - All metadata as JSON, returned with ``returnMetadata``.
   * - ``m_<key>``
     - ``ATOM``, ``DOUBLE`` or ``BOOLEAN``, multivalued
     - One field per filterable metadata key, added the first time the key is seen. The first value
       sets the type (string, number or boolean); later values of a different type are rejected.

Writes refresh the index before they return, so the next query sees them.

Idle index tiering
------------------

When ``--idleStopSeconds`` is set, the gateway checks every ``--idleCheckSeconds`` for indexes that have had no
requests for that long. It commits each one, which uploads all of its data to S3, and then stops
it. The next request to the index starts it again with ``startIndexV2``, which restores the data
from S3. Requests in progress always finish before an index is stopped.

This lets one nrtsearch cluster hold many more indexes than fit in its memory and disk at once,
which is how S3 Vectors keeps rarely used indexes cheap.

Lazy full precision vectors on replicas
---------------------------------------

Quantized vector search keeps three files per segment: the HNSW graph (``.vex``), the quantized
vectors (``.veq``) and the full precision vectors (``.vec``). The graph and quantized vectors are
read for every query, but the full precision vectors are only read to rescore the top
``topK * rescoreOversample`` hits. Full precision vectors take 4 bytes per dimension and 7 bit
quantized vectors about 1, so the ``.vec`` files are about 80% of the vector data (more with fewer
bits).

Isolated replicas (``isolatedReplicaConfig.enabled: true``) can leave the ``.vec`` files in S3:

.. code-block:: yaml

  isolatedReplicaConfig:
    enabled: true
  lazyRemoteData:
    enabled: true
    extensions: [vec]
    cacheType: MEMORY   # OFF_HEAP for a large cache outside the heap, DISK for larger than memory
    cacheSizeMB: 512
    blockSizeKB: 64

When the replica restores or copies an index version, each ``.vec`` file is written as a small
stub holding its S3 location, length, header and footer. Lucene sees a normal file: opening it and
checking its identity needs no S3 read. Rescoring reads the blocks it needs with S3 byte range
requests, and keeps them in an LRU block cache shared by the node: on the heap by default, in
native memory outside the heap (``OFF_HEAP``), or in local files (``DISK``).

Files inside compound (``.cfs``) files can't be lazy, so the gateway creates indexes with compound
files turned off (``useCompoundFile: false``, ``noCFSRatio: 0``). Use ``--compoundFiles`` to keep
them.

Cache metrics are exported on ``/status/metrics`` with a ``cache_type`` label:

.. list-table::
   :header-rows: 1

   * - Metric
     - Description
   * - ``nrt_lazy_remote_cache_hits_total``, ``nrt_lazy_remote_cache_misses_total``
     - Block reads served from the cache, and loaded from S3.
   * - ``nrt_lazy_remote_cache_remote_bytes_total``
     - Bytes read from S3.
   * - ``nrt_lazy_remote_cache_remote_load_seconds_total``
     - Time spent loading blocks from S3. Divide by misses for the average load latency.
   * - ``nrt_lazy_remote_cache_load_errors_total``
     - Failed block loads.
   * - ``nrt_lazy_remote_cache_evictions_total``
     - Blocks evicted to stay within ``cacheSizeMB``.
   * - ``nrt_lazy_remote_cache_size_bytes``, ``nrt_lazy_remote_cache_blocks``
     - Current cache size.
   * - ``nrt_lazy_remote_cache_max_bytes``, ``nrt_lazy_remote_cache_block_size_bytes``
     - Configured limits.
   * - ``nrt_lazy_remote_cache_allocated_bytes``
     - Memory or disk held by the cache. For ``OFF_HEAP`` this is the allocated slabs, including
       free slots; it grows to ``max_bytes`` and stays there.

Trade offs:

* Replicas need several times less disk and page cache for vector data: about 5x with 7 bit
  quantization, before counting the HNSW graph.
* Rescoring cache misses add S3 latency, typically tens of milliseconds per query. Blocks that are
  read often stay cached.
* Every block read is an S3 GET request.

Differences from S3 Vectors
---------------------------

* No IAM, encryption settings, tags or ARNs. Buckets and indexes are addressed only by name.
* No per-index rate limits.
* ``ListVectors`` parallel segments (``segmentCount``, ``segmentIndex``) are not supported.
* A metadata key has one type per index; S3 Vectors allows different types for the same key.
* The first query after an index has been stopped waits for it to be restored from S3.
