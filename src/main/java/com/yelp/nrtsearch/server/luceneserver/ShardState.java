/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.DeadlineUtils;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler.SearchHandlerException;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef.FacetValueType;
import com.yelp.nrtsearch.server.luceneserver.field.properties.GlobalOrdinalable;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import com.yelp.nrtsearch.server.luceneserver.index.NrtIndexWriter;
import com.yelp.nrtsearch.server.luceneserver.warming.WarmerConfig;
import com.yelp.nrtsearch.server.monitoring.IndexMetrics;
import com.yelp.nrtsearch.server.utils.FileUtil;
import com.yelp.nrtsearch.server.utils.HostPort;
import io.grpc.StatusRuntimeException;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.CachedOrdinalsReader;
import org.apache.lucene.facet.taxonomy.DocValuesOrdinalsReader;
import org.apache.lucene.facet.taxonomy.OrdinalsReader;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PersistentSnapshotDeletionPolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherLifetimeManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardState implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(ShardState.class);
  public static final int REPLICA_ID = 0;
  public static final String INDEX_DATA_DIR_NAME = "index";
  public static final String TAXONOMY_DATA_DIR_NAME = "taxonomy";
  final ThreadPoolExecutor searchExecutor;

  /** {@link IndexStateManager} for the index this shard belongs to */
  private final IndexStateManager indexStateManager;

  /** Where Lucene's index is written */
  private final Path rootDir;

  /** Which shard we are in this index */
  private final int shardOrd;

  /** Base directory */
  public Directory origIndexDir;

  /** Possibly NRTCachingDir wrap of origIndexDir */
  public Directory indexDir;

  /** Taxonomy directory */
  private Directory taxoDir;

  /** Only non-null for "ordinary" (not replicated) index */
  public IndexWriter writer;

  /** Only non-null if we are primary NRT replication index */
  // nocommit make private again, add methods to do stuff to it:
  public NRTPrimaryNode nrtPrimaryNode;

  /** Only non-null if we are replica NRT replication index */
  // nocommit make private again, add methods to do stuff to it:
  public NRTReplicaNode nrtReplicaNode;

  /** Taxonomy writer */
  public DirectoryTaxonomyWriter taxoWriter;

  /**
   * Internal IndexWriter used by DirectoryTaxonomyWriter; we pull this out so we can
   * .deleteUnusedFiles after a snapshot is removed.
   */
  public IndexWriter taxoInternalWriter;

  /** Maps snapshot gen -&gt; version. */
  public final Map<Long, Long> snapshotGenToVersion = new ConcurrentHashMap<>();

  /**
   * Holds cached ordinals; doesn't use any RAM unless it's actually used when a caller sets
   * useOrdsCache=true.
   */
  private final Map<String, OrdinalsReader> ordsCache = new HashMap<>();

  /**
   * Enables lookup of previously used searchers, so follow-on actions (next page, drill
   * down/sideways/up, etc.) use the same searcher as the original search, as long as that searcher
   * hasn't expired.
   */
  public volatile SearcherLifetimeManager slm = new SearcherLifetimeManager();

  /** Indexes changes, and provides the live searcher, possibly searching a specific generation. */
  private SearcherTaxonomyManager manager;

  private ReferenceManager<IndexSearcher> searcherManager;

  /** Thread to periodically reopen the index. */
  private ControlledRealTimeReopenThread<SearcherTaxonomyManager.SearcherAndTaxonomy> reopenThread;

  /** Used with NRT replication */
  private ControlledRealTimeReopenThread<IndexSearcher> reopenThreadPrimary;

  /** Periodically wakes up and prunes old searchers from slm. */
  private SearcherPruningThread searcherPruningThread;

  /** Holds the persistent snapshots */
  public PersistentSnapshotDeletionPolicy snapshots;

  /** Holds the persistent taxonomy snapshots */
  public PersistentSnapshotDeletionPolicy taxoSnapshots;

  private boolean doCreate;

  public final Map<IndexReader.CacheKey, Map<String, SortedSetDocValuesReaderState>> ssdvStates =
      new HashMap<>();
  private final Object ordinalBuilderLock = new Object();

  private final String name;
  private KeepAlive keepAlive;
  // is this shard restored
  private boolean restored;
  private volatile boolean started = false;

  public static String getShardDirectoryName(int shardOrd) {
    return "shard" + shardOrd;
  }

  /**
   * Notify the shard that the index live settings have been updated and provide the update {@link
   * IndexLiveSettings} message.
   *
   * @param updateMessage updated settings message
   */
  public void updatedLiveSettings(IndexLiveSettings updateMessage) {
    if (isStarted()) {
      if (updateMessage.hasMaxRefreshSec() || updateMessage.hasMinRefreshSec()) {
        logger.info("Restarting reopen thread");
        restartReopenThread();
      }
      IndexState currentState = indexStateManager.getCurrent();
      if (updateMessage.hasIndexRamBufferSizeMB()) {
        if (writer != null) {
          logger.info("Setting ram buffer size");
          writer.getConfig().setRAMBufferSizeMB(currentState.getIndexRamBufferSizeMB());
        }
      }
    }
  }

  /** Restarts the reopen thread (called when the live settings have changed). */
  public void restartReopenThread() {
    IndexState indexState = indexStateManager.getCurrent();
    if (reopenThread != null) {
      reopenThread.close();
    }
    if (reopenThreadPrimary != null) {
      reopenThreadPrimary.close();
    }
    // nocommit sync
    if (nrtPrimaryNode != null) {
      assert manager == null;
      assert searcherManager != null;
      assert nrtReplicaNode == null;
      // nocommit how to get taxonomy back?
      reopenThreadPrimary =
          new ControlledRealTimeReopenThread<>(
              writer,
              searcherManager,
              indexState.getMaxRefreshSec(),
              indexState.getMinRefreshSec());
      reopenThreadPrimary.setName("LuceneNRTPrimaryReopen-" + name);
      reopenThreadPrimary.start();
    } else if (manager != null) {
      if (reopenThread != null) {
        reopenThread.close();
      }
      reopenThread =
          new ControlledRealTimeReopenThread<>(
              writer, manager, indexState.getMaxRefreshSec(), indexState.getMinRefreshSec());
      reopenThread.setName("LuceneNRTReopen-" + name);
      reopenThread.start();
    }
  }

  /** True if this index is started. */
  public boolean isStarted() {
    if (started) {
      return isReplica() || (writer != null && writer.isOpen());
    }
    return false;
  }

  public boolean isRestored() {
    return restored;
  }

  public void setRestored(boolean restored) {
    this.restored = restored;
  }

  public String getState() {
    // TODO FIX ME: should it be read-only, etc?
    return isStarted() ? "started" : "not started";
  }

  /** Delete this shard. */
  public void deleteShard() throws IOException {
    if (rootDir != null) {
      FileUtil.deleteAllFiles(rootDir);
    }
  }

  public boolean isPrimary() {
    return nrtPrimaryNode != null;
  }

  public boolean isReplica() {
    return nrtReplicaNode != null;
  }

  public void waitForGeneration(long gen) throws InterruptedException {
    if (nrtPrimaryNode != null) {
      reopenThreadPrimary.waitForGeneration(gen);
    } else {
      reopenThread.waitForGeneration(gen);
    }
  }

  /**
   * Constructor.
   *
   * @param indexStateManager state manager for index
   * @param indexName index name
   * @param rootDir this index data root directory
   * @param searchExecutor search thread pool
   * @param shardOrd shard number
   * @param doCreate if index should be created when started
   */
  public ShardState(
      IndexStateManager indexStateManager,
      String indexName,
      Path rootDir,
      ThreadPoolExecutor searchExecutor,
      int shardOrd,
      boolean doCreate) {
    this.indexStateManager = indexStateManager;
    this.shardOrd = shardOrd;
    if (rootDir == null) {
      this.rootDir = null;
    } else {
      this.rootDir = rootDir.resolve(getShardDirectoryName(shardOrd));
    }
    this.name = indexName + ":" + shardOrd;
    this.doCreate = doCreate;
    this.searchExecutor = searchExecutor;
  }

  @Override
  public synchronized void close() throws IOException {
    logger.info(String.format("ShardState.close name= %s", name));

    if (writer != null && writer.isOpen()) {
      commit();
    }

    started = false;
    List<Closeable> closeables = new ArrayList<>();
    // nocommit catch exc & rollback:
    if (nrtPrimaryNode != null) {
      closeables.add(reopenThreadPrimary);
      closeables.add(searcherManager);
      // this closes writer:
      closeables.add(nrtPrimaryNode);
      closeables.add(searcherPruningThread);
      closeables.add(slm);
      closeables.add(indexDir);
      closeables.add(taxoDir);
      nrtPrimaryNode = null;
      writer = null;
    } else if (nrtReplicaNode != null) {
      closeables.add(keepAlive);
      closeables.add(reopenThreadPrimary);
      closeables.add(searcherManager);
      closeables.add(nrtReplicaNode);
      closeables.add(searcherPruningThread);
      closeables.add(slm);
      closeables.add(indexDir);
      closeables.add(taxoDir);
      nrtReplicaNode = null;
    } else if (writer != null) {
      closeables.add(reopenThread);
      closeables.add(manager);
      closeables.add(searcherPruningThread);
      closeables.add(slm);
      closeables.add(writer);
      closeables.add(taxoWriter);
      closeables.add(indexDir);
      closeables.add(taxoDir);
      writer = null;
    }
    slm = new SearcherLifetimeManager();

    IOUtils.close(closeables);
  }

  /** Set if the index should be created on next start. */
  public void setDoCreate(boolean doCreate) {
    this.doCreate = doCreate;
  }

  /** Commit all state. */
  public synchronized long commit() throws IOException {
    // This request may already have timed out on the client while waiting for the lock.
    // If so, there is no reason to continue this heavyweight operation.
    DeadlineUtils.checkDeadline("ShardState: commit " + this.name, "COMMIT");

    long gen;

    // nocommit this does nothing on replica?  make a failing test!
    if (writer != null) {
      // nocommit: two phase commit?
      if (taxoWriter != null) {
        taxoWriter.commit();
      }
      gen = writer.commit();
    } else {
      gen = -1;
    }

    return gen;
  }

  public SearcherTaxonomyManager.SearcherAndTaxonomy acquire() throws IOException {
    if (nrtPrimaryNode != null) {
      return new SearcherTaxonomyManager.SearcherAndTaxonomy(
          nrtPrimaryNode.getSearcherManager().acquire(), null);
    } else if (nrtReplicaNode != null) {
      return new SearcherTaxonomyManager.SearcherAndTaxonomy(
          nrtReplicaNode.getSearcherManager().acquire(), null);
    } else {
      return manager.acquire();
    }
  }

  public void release(SearcherTaxonomyManager.SearcherAndTaxonomy s) throws IOException {
    if (nrtPrimaryNode != null) {
      nrtPrimaryNode.getSearcherManager().release(s.searcher);
    } else if (nrtReplicaNode != null) {
      nrtReplicaNode.getSearcherManager().release(s.searcher);
    } else {
      manager.release(s);
    }
  }

  /** Prunes stale searchers. */
  private class SearcherPruningThread extends Thread implements Closeable {
    private final CountDownLatch shutdownNow;
    private volatile boolean done = false;

    /** Sole constructor. */
    public SearcherPruningThread(CountDownLatch shutdownNow) {
      this.shutdownNow = shutdownNow;
    }

    @Override
    public void run() {
      while (!done) {
        try {
          final SearcherLifetimeManager.Pruner byAge =
              new SearcherLifetimeManager.PruneByAge(
                  indexStateManager.getCurrent().getMaxSearcherAgeSec());
          final Set<Long> snapshots = new HashSet<>(snapshotGenToVersion.values());
          slm.prune(
              (ageSec, searcher) -> {
                long version = ((DirectoryReader) searcher.getIndexReader()).getVersion();
                if (snapshots.contains(version)) {
                  // Never time-out searcher for a snapshot:
                  return false;
                } else {
                  return byAge.doPrune(ageSec, searcher);
                }
              });
        } catch (IOException ioe) {
          // nocommit log
        }
        try {
          if (shutdownNow.await(1, TimeUnit.SECONDS)) {
            break;
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
      }
    }

    @Override
    public void close() throws IOException {
      done = true;
    }
  }

  /** Start the searcher pruning thread. */
  private void startSearcherPruningThread(CountDownLatch shutdownNow) {
    // nocommit make one thread in GlobalState
    if (searcherPruningThread == null) {
      searcherPruningThread = new SearcherPruningThread(shutdownNow);
      searcherPruningThread.setName("LuceneSearcherPruning-" + name);
      searcherPruningThread.start();
    }
  }

  /**
   * Factory class that produces a new searcher for each {@link IndexReader} version. Sets field
   * similarity and handles any eager global ordinal loading.
   */
  private class ShardSearcherFactory extends SearcherFactory {
    private final boolean loadEagerOrdinals;
    private final boolean collectMetrics;

    /**
     * Constructor.
     *
     * @param loadEagerOrdinals is eager global ordinal loading enabled, not needed for primary
     * @param collectMetrics if metrics should be collected for each new index searcher
     */
    ShardSearcherFactory(boolean loadEagerOrdinals, boolean collectMetrics) {
      this.loadEagerOrdinals = loadEagerOrdinals;
      this.collectMetrics = collectMetrics;
    }

    @Override
    public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader)
        throws IOException {
      IndexState indexState = indexStateManager.getCurrent();
      IndexSearcher searcher =
          new MyIndexSearcher(
              reader,
              new MyIndexSearcher.ExecutorWithParams(
                  searchExecutor,
                  indexState.getSliceMaxDocs(),
                  indexState.getSliceMaxSegments(),
                  indexState.getVirtualShards()));
      searcher.setSimilarity(indexState.searchSimilarity);
      if (loadEagerOrdinals) {
        loadEagerGlobalOrdinals(reader, indexState);
      }
      if (collectMetrics) {
        IndexMetrics.updateReaderStats(indexState.getName(), reader);
        IndexMetrics.updateSearcherStats(indexState.getName(), searcher);
      }
      return searcher;
    }

    private void loadEagerGlobalOrdinals(IndexReader reader, IndexState indexState)
        throws IOException {
      for (Map.Entry<String, FieldDef> entry :
          indexState.getEagerGlobalOrdinalFields().entrySet()) {
        // only sorted set doc values facet currently supported
        if (entry.getValue().getFacetValueType() == FacetValueType.SORTED_SET_DOC_VALUES) {
          // get state to populate cache
          getSSDVStateForReader(indexState, reader, entry.getValue());
        } else {
          logger.warn(
              String.format(
                  "Field: %s, facet type: %s, does not support eager global ordinals",
                  entry.getKey(), entry.getValue().getFacetValueType().toString()));
        }
      }

      for (Map.Entry<String, GlobalOrdinalable> entry :
          indexState.getEagerFieldGlobalOrdinalFields().entrySet()) {
        if (entry.getValue().usesOrdinals()) {
          // get lookup to populate cache
          entry.getValue().getOrdinalLookup(reader);
        }
      }
    }
  }

  /** Start this shard as standalone (not primary nor replica) */
  public synchronized void start() throws IOException {

    if (isStarted()) {
      throw new IllegalStateException("index \"" + name + "\" was already started");
    }
    IndexState indexState = indexStateManager.getCurrent();

    try {
      Path indexDirFile;
      if (rootDir == null) {
        indexDirFile = null;
      } else {
        indexDirFile = rootDir.resolve(INDEX_DATA_DIR_NAME);
      }
      origIndexDir =
          indexState
              .getDirectoryFactory()
              .open(
                  indexDirFile, indexState.getGlobalState().getConfiguration().getPreloadConfig());

      // nocommit don't allow RAMDir
      // nocommit remove NRTCachingDir too?
      if ((origIndexDir instanceof MMapDirectory) == false) {
        double maxMergeSizeMB = indexState.getNrtCachingDirectoryMaxMergeSizeMB();
        double maxSizeMB = indexState.getNrtCachingDirectoryMaxSizeMB();
        if (maxMergeSizeMB > 0 && maxSizeMB > 0) {
          indexDir = new NRTCachingDirectory(origIndexDir, maxMergeSizeMB, maxSizeMB);
        } else {
          indexDir = origIndexDir;
        }
      } else {
        indexDir = origIndexDir;
      }

      // Rather than rely on IndexWriter/TaxonomyWriter to
      // figure out if an index is new or not by passing
      // CREATE_OR_APPEND (which can be dangerous), we
      // already know the intention from the app (whether
      // it called createIndex vs openIndex), so we make it
      // explicit here:
      IndexWriterConfig.OpenMode openMode;
      if (doCreate) {
        // nocommit shouldn't we set doCreate=false after we've done the create?  make test!
        openMode = IndexWriterConfig.OpenMode.CREATE;
      } else {
        openMode = IndexWriterConfig.OpenMode.APPEND;
      }

      Path taxoDirFile;
      if (rootDir == null) {
        taxoDirFile = null;
      } else {
        taxoDirFile = rootDir.resolve(TAXONOMY_DATA_DIR_NAME);
      }
      taxoDir =
          indexState
              .getDirectoryFactory()
              .open(taxoDirFile, indexState.getGlobalState().getConfiguration().getPreloadConfig());

      taxoSnapshots =
          new PersistentSnapshotDeletionPolicy(
              new KeepOnlyLastCommitDeletionPolicy(),
              taxoDir,
              IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

      taxoWriter =
          new DirectoryTaxonomyWriter(taxoDir, OpenMode.CREATE_OR_APPEND) {
            @Override
            protected IndexWriterConfig createIndexWriterConfig(
                IndexWriterConfig.OpenMode openMode) {
              IndexWriterConfig iwc = super.createIndexWriterConfig(openMode);
              iwc.setIndexDeletionPolicy(taxoSnapshots);
              return iwc;
            }

            @Override
            protected IndexWriter openIndexWriter(Directory dir, IndexWriterConfig iwc)
                throws IOException {
              IndexWriter w = super.openIndexWriter(dir, iwc);
              taxoInternalWriter = w;
              return w;
            }
          };

      writer =
          new NrtIndexWriter(
              indexDir,
              indexState.getIndexWriterConfig(openMode, origIndexDir, shardOrd),
              indexState.getName());
      snapshots = (PersistentSnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();

      // NOTE: must do this after writer, because SDP only
      // loads its commits after writer calls .onInit:
      for (IndexCommit c : snapshots.getSnapshots()) {
        long gen = c.getGeneration();
        SegmentInfos sis =
            SegmentInfos.readCommit(
                origIndexDir,
                IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
        snapshotGenToVersion.put(c.getGeneration(), sis.getVersion());
      }

      // nocommit must also pull snapshots for taxoReader?

      manager =
          new SearcherTaxonomyManager(
              writer, true, new ShardSearcherFactory(true, true), taxoWriter);

      restartReopenThread();

      startSearcherPruningThread(indexState.getGlobalState().getShutdownLatch());
      started = true;
    } finally {
      if (!started) {
        IOUtils.closeWhileHandlingException(
            reopenThread,
            manager,
            writer,
            taxoWriter,
            searcherPruningThread,
            slm,
            indexDir,
            taxoDir);
        writer = null;
        slm = new SearcherLifetimeManager();
      }
    }
  }

  /**
   * Start this index as primary, to NRT-replicate to replicas. primaryGen should increase each time
   * a new primary is promoted for a given index.
   *
   * @param primaryGen generation to use for {@link NRTPrimaryNode}, uses value from global state if
   *     -1
   */
  public synchronized void startPrimary(long primaryGen) throws IOException {
    if (isStarted()) {
      throw new IllegalStateException("index \"" + name + "\" was already started");
    }
    IndexState indexState = indexStateManager.getCurrent();
    // nocommit share code better w/ start and startReplica!

    try {
      Path indexDirFile;
      if (rootDir == null) {
        indexDirFile = null;
      } else {
        indexDirFile = rootDir.resolve(INDEX_DATA_DIR_NAME);
      }
      origIndexDir =
          indexState
              .getDirectoryFactory()
              .open(
                  indexDirFile, indexState.getGlobalState().getConfiguration().getPreloadConfig());

      if ((origIndexDir instanceof MMapDirectory) == false) {
        double maxMergeSizeMB = indexState.getNrtCachingDirectoryMaxMergeSizeMB();
        double maxSizeMB = indexState.getNrtCachingDirectoryMaxSizeMB();
        if (maxMergeSizeMB > 0 && maxSizeMB > 0) {
          indexDir = new NRTCachingDirectory(origIndexDir, maxMergeSizeMB, maxSizeMB);
        } else {
          indexDir = origIndexDir;
        }
      } else {
        indexDir = origIndexDir;
      }

      // Rather than rely on IndexWriter/TaxonomyWriter to
      // figure out if an index is new or not by passing
      // CREATE_OR_APPEND (which can be dangerous), we
      // already know the intention from the app (whether
      // it called createIndex vs openIndex), so we make it
      // explicit here:
      IndexWriterConfig.OpenMode openMode;
      if (doCreate) {
        // nocommit shouldn't we set doCreate=false after we've done the create?
        openMode = IndexWriterConfig.OpenMode.CREATE;
      } else {
        openMode = IndexWriterConfig.OpenMode.APPEND;
      }

      // TODO: get facets working!

      boolean verbose = indexState.getGlobalState().getConfiguration().getIndexVerbose();

      writer =
          new NrtIndexWriter(
              indexDir,
              indexState.getIndexWriterConfig(openMode, origIndexDir, shardOrd),
              indexState.getName());
      LiveIndexWriterConfig writerConfig = writer.getConfig();
      MergePolicy mergePolicy = writerConfig.getMergePolicy();
      // Disable merges while NrtPrimaryNode isn't initalized (ISSUE-210)
      writerConfig.setMergePolicy(NoMergePolicy.INSTANCE);
      snapshots = (PersistentSnapshotDeletionPolicy) writerConfig.getIndexDeletionPolicy();

      // NOTE: must do this after writer, because SDP only
      // loads its commits after writer calls .onInit:
      for (IndexCommit c : snapshots.getSnapshots()) {
        long gen = c.getGeneration();
        SegmentInfos sis =
            SegmentInfos.readCommit(
                origIndexDir,
                IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
        snapshotGenToVersion.put(c.getGeneration(), sis.getVersion());
      }

      long resolvedPrimaryGen =
          primaryGen == -1 ? indexState.getGlobalState().getGeneration() : primaryGen;
      HostPort hostPort =
          new HostPort(
              indexState.getGlobalState().getHostName(),
              indexState.getGlobalState().getReplicationPort());
      nrtPrimaryNode =
          new NRTPrimaryNode(
              indexStateManager,
              hostPort,
              writer,
              0,
              resolvedPrimaryGen,
              -1,
              new ShardSearcherFactory(false, true),
              verbose ? System.out : new PrintStream(OutputStream.nullOutputStream()));
      // Enable merges
      writerConfig.setMergePolicy(mergePolicy);

      // nocommit this isn't used?
      searcherManager =
          new NRTPrimaryNode.PrimaryNodeReferenceManager(
              nrtPrimaryNode,
              new SearcherFactory() {
                @Override
                public IndexSearcher newSearcher(IndexReader r, IndexReader previousReader) {
                  IndexSearcher searcher =
                      new MyIndexSearcher(
                          r,
                          new MyIndexSearcher.ExecutorWithParams(
                              searchExecutor,
                              indexState.getSliceMaxDocs(),
                              indexState.getSliceMaxSegments(),
                              indexState.getVirtualShards()));
                  searcher.setSimilarity(indexState.searchSimilarity);
                  return searcher;
                }
              });
      restartReopenThread();

      startSearcherPruningThread(indexState.getGlobalState().getShutdownLatch());
      started = true;
    } finally {
      if (!started) {
        IOUtils.closeWhileHandlingException(
            reopenThread,
            nrtPrimaryNode,
            writer,
            taxoWriter,
            searcherPruningThread,
            slm,
            indexDir,
            taxoDir);
        writer = null;
        slm = new SearcherLifetimeManager();
      }
    }
  }

  private final IndexReader.ClosedListener removeSSDVStates =
      cacheKey -> {
        synchronized (ssdvStates) {
          ssdvStates.remove(cacheKey);
        }
      };

  /** Returns cached ordinals for the specified index field name. */
  public synchronized OrdinalsReader getOrdsCache(String indexFieldName) {
    OrdinalsReader ords = ordsCache.get(indexFieldName);
    if (ords == null) {
      ords = new CachedOrdinalsReader(new DocValuesOrdinalsReader(indexFieldName));
      ordsCache.put(indexFieldName, ords);
    }
    return ords;
  }

  public SortedSetDocValuesReaderState getSSDVState(
      IndexState indexState, SearcherTaxonomyManager.SearcherAndTaxonomy s, FieldDef fd)
      throws IOException {
    return getSSDVStateForReader(indexState, s.searcher.getIndexReader(), fd);
  }

  public SortedSetDocValuesReaderState getSSDVStateForReader(
      IndexState indexState, IndexReader reader, FieldDef fd) throws IOException {
    FacetsConfig.DimConfig dimConfig = indexState.getFacetsConfig().getDimConfig(fd.getName());
    IndexReader.CacheKey cacheKey = reader.getReaderCacheHelper().getKey();
    SortedSetDocValuesReaderState ssdvState;
    synchronized (ssdvStates) {
      Map<String, SortedSetDocValuesReaderState> readerSSDVStates = ssdvStates.get(cacheKey);
      if (readerSSDVStates == null) {
        readerSSDVStates = new HashMap<>();
        ssdvStates.put(cacheKey, readerSSDVStates);
        reader.getReaderCacheHelper().addClosedListener(removeSSDVStates);
      }

      ssdvState = readerSSDVStates.get(dimConfig.indexFieldName);
    }

    if (ssdvState == null) {
      // Lock building SSDV state with different lock, so it won't block readers accessing
      // the cache. Uses a single lock for now, could be made better by scoping it to the
      // field and reader key.
      synchronized (ordinalBuilderLock) {
        // make sure state was not built while we were waiting for the lock
        synchronized (ssdvStates) {
          Map<String, SortedSetDocValuesReaderState> readerSSDVStates = ssdvStates.get(cacheKey);
          if (readerSSDVStates == null) {
            // we added this above, so we really should never get here
            throw new IllegalStateException("SSDV State cache does not exist for reader");
          }
          ssdvState = readerSSDVStates.get(dimConfig.indexFieldName);
        }
        if (ssdvState == null) {
          ssdvState =
              new DefaultSortedSetDocValuesReaderState(reader, dimConfig.indexFieldName) {
                @Override
                public SortedSetDocValues getDocValues() throws IOException {
                  SortedSetDocValues values = super.getDocValues();
                  if (values == null) {
                    values = DocValues.emptySortedSet();
                  }
                  return values;
                }

                @Override
                public OrdRange getOrdRange(String dim) {
                  OrdRange result = super.getOrdRange(dim);
                  if (result == null) {
                    result = new OrdRange(0, -1);
                  }
                  return result;
                }
              };

          // add state to cache
          synchronized (ssdvStates) {
            Map<String, SortedSetDocValuesReaderState> readerSSDVStates = ssdvStates.get(cacheKey);
            if (readerSSDVStates == null) {
              // we added this above, so we really should never get here
              throw new IllegalStateException("SSDV State cache does not exist for reader");
            }
            readerSSDVStates.put(dimConfig.indexFieldName, ssdvState);
          }
        }
      }
    }

    return ssdvState;
  }

  /**
   * Start this index as replica, pulling NRT changes from the specified primary.
   *
   * @param primaryAddress client to communicate with primary replication server
   * @param primaryGen last primary generation, or -1 to detect from index
   */
  public synchronized void startReplica(ReplicationServerClient primaryAddress, long primaryGen)
      throws IOException {
    if (isStarted()) {
      throw new IllegalStateException("index \"" + name + "\" was already started");
    }
    IndexState indexState = indexStateManager.getCurrent();
    LuceneServerConfiguration configuration = indexState.getGlobalState().getConfiguration();

    // nocommit share code better w/ start and startPrimary!
    try {
      Path indexDirFile;
      if (rootDir == null) {
        indexDirFile = null;
      } else {
        indexDirFile = rootDir.resolve(INDEX_DATA_DIR_NAME);
      }
      origIndexDir =
          indexState.getDirectoryFactory().open(indexDirFile, configuration.getPreloadConfig());
      // nocommit don't allow RAMDir
      // nocommit remove NRTCachingDir too?
      if ((origIndexDir instanceof MMapDirectory) == false) {
        double maxMergeSizeMB = indexState.getNrtCachingDirectoryMaxMergeSizeMB();
        double maxSizeMB = indexState.getNrtCachingDirectoryMaxSizeMB();
        if (maxMergeSizeMB > 0 && maxSizeMB > 0) {
          indexDir = new NRTCachingDirectory(origIndexDir, maxMergeSizeMB, maxSizeMB);
        } else {
          indexDir = origIndexDir;
        }
      } else {
        indexDir = origIndexDir;
      }

      manager = null;
      nrtPrimaryNode = null;

      boolean verbose = configuration.getIndexVerbose();

      HostPort hostPort =
          new HostPort(
              indexState.getGlobalState().getHostName(),
              indexState.getGlobalState().getReplicationPort());
      nrtReplicaNode =
          new NRTReplicaNode(
              indexState.getName(),
              primaryAddress,
              hostPort,
              REPLICA_ID,
              indexDir,
              new ShardSearcherFactory(true, false),
              verbose ? System.out : new PrintStream(OutputStream.nullOutputStream()),
              configuration.getFileCopyConfig().getAckedCopy(),
              configuration.getDecInitialCommit(),
              configuration.getFilterIncompatibleSegmentReaders());
      if (primaryGen != -1) {
        nrtReplicaNode.start(primaryGen);
      } else {
        nrtReplicaNode.startWithLastPrimaryGen();
      }

      if (configuration.getSyncInitialNrtPoint()) {
        nrtReplicaNode.syncFromCurrentPrimary(
            configuration.getInitialSyncPrimaryWaitMs(), configuration.getInitialSyncMaxTimeMs());
      }

      startSearcherPruningThread(indexState.getGlobalState().getShutdownLatch());

      // Necessary so that the replica "hang onto" all versions sent to it, since the version is
      // sent back to the user on writeNRTPoint
      addRefreshListener(
          new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {}

            @Override
            public void afterRefresh(boolean didRefresh) throws IOException {
              SearcherTaxonomyManager.SearcherAndTaxonomy current = acquire();
              try {
                slm.record(current.searcher);
              } finally {
                release(current);
              }
            }
          });
      keepAlive = new KeepAlive(this);
      new Thread(keepAlive, "KeepAlive").start();

      WarmerConfig warmerConfig = configuration.getWarmerConfig();
      if (warmerConfig.isWarmOnStartup() && indexState.getWarmer() != null) {
        try {
          indexState.getWarmer().warmFromS3(indexState, warmerConfig.getWarmingParallelism());
        } catch (SearchHandlerException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      started = true;
    } finally {
      if (!started) {
        IOUtils.closeWhileHandlingException(
            reopenThread,
            nrtReplicaNode,
            writer,
            taxoWriter,
            searcherPruningThread,
            slm,
            indexDir,
            taxoDir);
        writer = null;
        slm = new SearcherLifetimeManager();
      }
    }
  }

  public void addRefreshListener(ReferenceManager.RefreshListener listener) {
    if (nrtPrimaryNode != null) {
      nrtPrimaryNode.getSearcherManager().addListener(listener);
    } else if (nrtReplicaNode != null) {
      nrtReplicaNode.getSearcherManager().addListener(listener);
    } else {
      manager.addListener(listener);
    }
  }

  public void removeRefreshListener(ReferenceManager.RefreshListener listener) {
    if (nrtPrimaryNode != null) {
      nrtPrimaryNode.getSearcherManager().removeListener(listener);
    } else if (nrtReplicaNode != null) {
      nrtReplicaNode.getSearcherManager().removeListener(listener);
    } else {
      manager.removeListener(listener);
    }
  }

  public void maybeRefreshBlocking() throws IOException {
    if (nrtPrimaryNode != null) {
      /* invokes: SearcherManager.refreshIfNeeded which creates a new Searcher
       * over a new IndexReader if new docs have been written */
      nrtPrimaryNode.getSearcherManager().maybeRefreshBlocking();
      /* Do we need this as well? (probably)
        invokes PrimaryNodeReferenceManager.refreshIfNeeded()
        The above method calls primary.flushAndRefresh() (which updates copyState on Primary) and primary.sendNewNRTPointToReplicas().
        This is also run in a separate thread every near-real-time interval (1s) (see: restartReopenThread for Primary)
      */
      searcherManager.maybeRefreshBlocking();
    } else {
      /* SearchAndTaxnomyManager for stand alone mode */
      manager.maybeRefreshBlocking();
    }
  }

  public static class KeepAlive implements Runnable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(KeepAlive.class);
    private volatile boolean exit = false;
    private final int pingIntervalMs;
    private final ShardState shardState;

    KeepAlive(ShardState shardState) {
      this.shardState = shardState;
      this.pingIntervalMs =
          shardState
              .indexStateManager
              .getCurrent()
              .getGlobalState()
              .getReplicaReplicationPortPingInterval();
    }

    @Override
    public void run() {
      while (!exit) {
        NRTReplicaNode nrtReplicaNode = shardState.nrtReplicaNode;
        try {
          TimeUnit.MILLISECONDS.sleep(pingIntervalMs);
          if (shardState.isReplica()
              && shardState.isStarted()
              && !shardState.nrtReplicaNode.isKnownToPrimary()
              && !exit) {
            nrtReplicaNode
                .getPrimaryAddress()
                .addReplicas(
                    shardState.indexStateManager.getCurrent().getName(),
                    REPLICA_ID,
                    nrtReplicaNode.getHostPort().getHostName(),
                    nrtReplicaNode.getHostPort().getPort());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (StatusRuntimeException e) {
          logger.warn(
              String.format(
                  "Replica host: %s, binary port: %s cannot reach primary: %s",
                  nrtReplicaNode.getHostPort().getHostName(),
                  nrtReplicaNode.getHostPort().getPort(),
                  nrtReplicaNode.getPrimaryAddress()));
        }
      }
    }

    @Override
    public void close() {
      exit = true;
    }
  }
}
