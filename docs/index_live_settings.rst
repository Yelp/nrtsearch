Index Live Settings
==========================

Index level properties that can be updated even when an index is started.

Applying
-----------------------------

Changes to these settings can be made any time after the index is created. There are several ways to do this:

* Directly use the `liveSetting/liveSettingV2 <https://github.com/Yelp/nrtsearch/blob/master/clientlib/src/main/proto/yelp/nrtsearch/luceneserver.proto#L35>`_ gRPC server endpoints
* Use the '/v1/live_settings' or '/v2/live_settings' endpoints with the gRPC gateway
* Use the lucene-client `liveSettings/liveSettingsV2 <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/cli/LuceneClientCommand.java>`_ commands

Properties
-----------------------------

minRefreshSec
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Minimum amount of time the index refresh thread will wait before opening a new searcher version, making the latest writes visible.

Must be <= maxRefreshSec

Default: 0.05

maxRefreshSec
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum amount of time the index refresh thread will wait before opening a new searcher version, making the latest writes visible.

Must be >= minRefreshSec

Default: 1.0

maxSearcherAgeSec
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum time before a searcher version is eligible for pruning, freeing its index resources.

Must be >= 0.0

Default: 60.0

indexRamBufferSizeMB
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Amount of data that will be buffered in memory during indexing. Surpassing this limit triggers an immediate flush of pending changes.

Must be > 0.0

Default: 16.0

addDocumentsMaxBufferLen
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Chunk size to use for indexing. This many documents will be processed together by a single thread.

Must be > 0

Default: 100

sliceMaxDocs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Used to determine how to divide index segments into parallel search slices. The slice is full once it has more than this many documents.

Must be > 0

Default: 250_000

sliceMaxSegments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Used to determine how to divide index segments into parallel search slices. The slice is full once it has more than this many segments.

Must be > 0

Default: 5

virtualShards
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Specifies the number of virtual shards for the index. This property has no effect if 'virtualSharding' is not enabled in the main config file. Groups the index segments into n buckets for the purposes of making merge decisions and creating parallel search slices, leading to more predictable parallelism.

This value should be set prior to indexing for best results. If changes are made afterwards, document distribution should eventually converge to the new sharding value through organic indexing, but this may take a while. A full re-indexing would be recommended.

Must be > 0

Default: 1

maxMergedSegmentMB
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum size of a segment produced from a merge operation.

Must be >= 0

Default: 5120 (5GB)

segmentsPerTier
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Number of segments per level for the `TieredMergePolicy <https://lucene.apache.org/core/8_4_0/core/org/apache/lucene/index/TieredMergePolicy.html>`_. Lowering this decreases the absolute segment count, at the cost of more merging.

Must be >= 2

Default: 10

defaultSearchTimeoutSec
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Specifies a default timeout to use for all search queries that do not specify one, or 0.0 for no timeout.

Must be >= 0.0

Default: 0.0

defaultSearchTimeoutCheckEvery
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Specifies the default number of documents to collect between checking if the search timeout has been reached, if not specified in the query. A value of 0 means only check on segment boundaries.

Must be >= 0

Default: 0

defaultTerminateAfter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Specifies the default number of documents to collect before terminating the search operation early, if not specified in the query. A value of 0 means no early termination.

Must be >= 0

Default: 0

maxMergePreCopyDurationSec
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Specifies the maximum time to wait for replicas to precopy merged segment files. If this time is exceeded the merge will continue without finishing the precopy. If set to 0 there would not be any time limit for precopy.

Must be >= 0

Default: 0