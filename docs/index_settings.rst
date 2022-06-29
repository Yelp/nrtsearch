Index Settings
==========================

Index level properties that can only be updated before the index is started.

Applying
-----------------------------

Changes to these settings can be made after the index is created, but before it is started. With the exception of indexSort, other setting may be changed any time the index is stopped. There are several ways to do this:

* Directly use the `setting/settingV2 <https://github.com/Yelp/nrtsearch/blob/master/clientlib/src/main/proto/yelp/nrtsearch/luceneserver.proto#L80>`_ gRPC server endpoints
* Use the '/v1/settings' or '/v2/settings' endpoints with the gRPC gateway
* Use the lucene-client `settings/settingsV2 <https://github.com/Yelp/nrtsearch/blob/master/src/main/java/com/yelp/nrtsearch/server/cli/LuceneClientCommand.java>`_ commands

Properties
-----------------------------

directory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Specifies the lucene `Directory <https://lucene.apache.org/core/8_4_0/core/org/apache/lucene/store/Directory.html>`_ to use for interacting with index data. Must be one of the following values:

* FSDirectory - Let lucene choose the best implementation for your system from the `FSDirectory <https://lucene.apache.org/core/8_4_0/core/org/apache/lucene/store/FSDirectory.html>`_ subclasses (MMapDirectory, NIOFSDirectory, SimpleFSDirectory).
* MMapDirectory - Uses memory-mapped IO when reading.
* NIOFSDirectory - Uses java.nio's FileChannel's positional io when reading to avoid synchronization when reading from the same file.
* SimpleFSDirectory - A straightforward implementation using Files.newByteChannel. However, it has poor concurrent performance (multiple threads will bottleneck) as it synchronizes when multiple threads read from the same file.
* RAMDirectory - Deprecated, see `lucene docs <https://lucene.apache.org/core/8_4_0/core/org/apache/lucene/store/RAMDirectory.html>`_.

Default: FSDirectory

indexSort
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Specify sorting for documents within each index segment. This should only be set prior to indexing any documents, and cannot be changed. The sort definition is the same as used for sort based search queries, except 'score' or 'docid' sorting are not allowed.

Default: none (lucene doc id)

concurrentMergeSchedulerMaxThreadCount
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Specifies max thread count for `ConcurrentMergeScheduler <https://lucene.apache.org/core/8_4_0/core/org/apache/lucene/index/ConcurrentMergeScheduler.html#setMaxMergesAndThreads-int-int->`_. This is the maximum pending merges allowed before indexing will block. Must be >= concurrentMergeSchedulerMaxMergeCount. May be set to -1 to auto detect, but concurrentMergeSchedulerMaxMergeCount must be set to auto detect as well.

Default: -1 (auto detect)

concurrentMergeSchedulerMaxMergeCount
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Specifies max merge count for `ConcurrentMergeScheduler <https://lucene.apache.org/core/8_4_0/core/org/apache/lucene/index/ConcurrentMergeScheduler.html#setMaxMergesAndThreads-int-int->`_. This is the maximum number of merges running concurrently. Must be <= concurrentMergeSchedulerMaxThreadCount. May be set to -1 to auto detect, but concurrentMergeSchedulerMaxThreadCount must be set to auto detect as well.

Default: -1 (auto detect)

indexMergeSchedulerAutoThrottle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Specifies if merge write io should be limited to just keep merge processing from falling behind.

Default: false

nrtCachingDirectoryMaxSizeMB
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When both this and nrtCachingDirectoryMaxMergeSizeMB are > 0 and the index directory is not an MMapDirectory, adds an `NRTCachingDirectory <https://lucene.apache.org/core/8_4_0/core/org/apache/lucene/store/NRTCachingDirectory.html>`_ wrapper. Specifies the maximum index data that can be cached.

Default: 60.0

nrtCachingDirectoryMaxMergeSizeMB
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When both this and nrtCachingDirectoryMaxSizeMB are > 0 and the index directory is not an MMapDirectory, adds an `NRTCachingDirectory <https://lucene.apache.org/core/8_4_0/core/org/apache/lucene/store/NRTCachingDirectory.html>`_ wrapper. Specifies the maximum size of merges that can be cached.

Default: 5.0