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

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.utils.HostPort;
import io.grpc.StatusRuntimeException;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.document.Document;
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
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.PersistentSnapshotDeletionPolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
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
  public static final int REPLICA_ID = 0;
  private final ThreadPoolExecutor searchExecutor;
  Logger logger = LoggerFactory.getLogger(ShardState.class);

  /** {@link IndexState} for the index this shard belongs to */
  public final IndexState indexState;

  /** Where Lucene's index is written */
  public final Path rootDir;

  /** Which shard we are in this index */
  public final int shardOrd;

  /** Base directory */
  public Directory origIndexDir;

  /** Possibly NRTCachingDir wrap of origIndexDir */
  public Directory indexDir;

  /** Taxonomy directory */
  Directory taxoDir;

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
  public final Map<Long, Long> snapshotGenToVersion = new ConcurrentHashMap<Long, Long>();

  /**
   * Holds cached ordinals; doesn't use any RAM unless it's actually used when a caller sets
   * useOrdsCache=true.
   */
  public final Map<String, OrdinalsReader> ordsCache = new HashMap<String, OrdinalsReader>();

  /**
   * Enables lookup of previously used searchers, so follow-on actions (next page, drill
   * down/sideways/up, etc.) use the same searcher as the original search, as long as that searcher
   * hasn't expired.
   */
  public final SearcherLifetimeManager slm = new SearcherLifetimeManager();

  /** Indexes changes, and provides the live searcher, possibly searching a specific generation. */
  private SearcherTaxonomyManager manager;

  private ReferenceManager<IndexSearcher> searcherManager;

  /** Thread to periodically reopen the index. */
  public ControlledRealTimeReopenThread<SearcherTaxonomyManager.SearcherAndTaxonomy> reopenThread;

  /** Used with NRT replication */
  public ControlledRealTimeReopenThread<IndexSearcher> reopenThreadPrimary;

  /** Periodically wakes up and prunes old searchers from slm. */
  Thread searcherPruningThread;

  /** Holds the persistent snapshots */
  public PersistentSnapshotDeletionPolicy snapshots;

  /** Holds the persistent taxonomy snapshots */
  public PersistentSnapshotDeletionPolicy taxoSnapshots;

  private final boolean doCreate;

  private final List<HostAndPort> replicas = new ArrayList<>();

  public final Map<IndexReader.CacheKey, Map<String, SortedSetDocValuesReaderState>> ssdvStates =
      new HashMap<>();

  public final String name;
  private KeepAlive keepAlive;
  // is this shard restored
  private boolean restored;

  /** Restarts the reopen thread (called when the live settings have changed). */
  public void restartReopenThread() {
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
          new ControlledRealTimeReopenThread<IndexSearcher>(
              writer, searcherManager, indexState.maxRefreshSec, indexState.minRefreshSec);
      reopenThreadPrimary.setName("LuceneNRTPrimaryReopen-" + name);
      reopenThreadPrimary.start();
    } else if (manager != null) {
      if (reopenThread != null) {
        reopenThread.close();
      }
      reopenThread =
          new ControlledRealTimeReopenThread<SearcherTaxonomyManager.SearcherAndTaxonomy>(
              writer, manager, indexState.maxRefreshSec, indexState.minRefreshSec);
      reopenThread.setName("LuceneNRTReopen-" + name);
      reopenThread.start();
    }
  }

  /** True if this index is started. */
  public boolean isStarted() {
    return writer != null || nrtReplicaNode != null || nrtPrimaryNode != null;
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
      deleteAllFiles(rootDir);
    }
  }

  private static void deleteAllFiles(Path dir) throws IOException {
    if (Files.exists(dir)) {
      if (Files.isRegularFile(dir)) {
        Files.delete(dir);
      } else {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
          for (Path path : stream) {
            if (Files.isDirectory(path)) {
              deleteAllFiles(path);
            } else {
              Files.delete(path);
            }
          }
        }
        Files.delete(dir);
      }
    }
  }

  public boolean isPrimary() {
    return nrtPrimaryNode != null;
  }

  public boolean isReplica() {
    return nrtReplicaNode != null;
  }

  public void waitForGeneration(long gen) throws InterruptedException, IOException {
    if (nrtPrimaryNode != null) {
      reopenThreadPrimary.waitForGeneration(gen);
    } else {
      reopenThread.waitForGeneration(gen);
    }
  }

  public static class HostAndPort {
    public final InetAddress host;
    public final int port;

    public HostAndPort(InetAddress host, int port) {
      this.host = host;
      this.port = port;
    }
  }

  public ShardState(IndexState indexState, int shardOrd, boolean doCreate) {
    this.indexState = indexState;
    this.shardOrd = shardOrd;
    if (indexState.rootDir == null) {
      this.rootDir = null;
    } else {
      this.rootDir = indexState.rootDir.resolve("shard" + shardOrd);
    }
    this.name = indexState.name + ":" + shardOrd;
    this.doCreate = doCreate;
    this.searchExecutor = indexState.getSearchThreadPoolExecutor();
  }

  @Override
  public synchronized void close() throws IOException {
    logger.info(String.format("ShardState.close name= %s", name));

    commit();

    List<Closeable> closeables = new ArrayList<Closeable>();
    // nocommit catch exc & rollback:
    if (nrtPrimaryNode != null) {
      closeables.add(reopenThreadPrimary);
      closeables.add(searcherManager);
      // this closes writer:
      closeables.add(nrtPrimaryNode);
      closeables.add(slm);
      closeables.add(indexDir);
      closeables.add(taxoDir);
      nrtPrimaryNode = null;
    } else if (nrtReplicaNode != null) {
      closeables.add(keepAlive);
      closeables.add(reopenThreadPrimary);
      closeables.add(searcherManager);
      closeables.add(nrtReplicaNode);
      closeables.add(slm);
      closeables.add(indexDir);
      closeables.add(taxoDir);
      nrtPrimaryNode = null;
    } else if (writer != null) {
      closeables.add(reopenThread);
      closeables.add(manager);
      closeables.add(slm);
      closeables.add(writer);
      closeables.add(taxoWriter);
      closeables.add(indexDir);
      closeables.add(taxoDir);
      writer = null;
    }

    IOUtils.close(closeables);
  }

  /** Commit all state. */
  public synchronized long commit() throws IOException {

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
  private class SearcherPruningThread extends Thread {
    private final CountDownLatch shutdownNow;

    /** Sole constructor. */
    public SearcherPruningThread(CountDownLatch shutdownNow) {
      this.shutdownNow = shutdownNow;
    }

    @Override
    public void run() {
      while (true) {
        try {
          final SearcherLifetimeManager.Pruner byAge =
              new SearcherLifetimeManager.PruneByAge(indexState.maxSearcherAgeSec);
          final Set<Long> snapshots = new HashSet<Long>(snapshotGenToVersion.values());
          slm.prune(
              new SearcherLifetimeManager.Pruner() {
                @Override
                public boolean doPrune(double ageSec, IndexSearcher searcher) {
                  long version = ((DirectoryReader) searcher.getIndexReader()).getVersion();
                  if (snapshots.contains(version)) {
                    // Never time-out searcher for a snapshot:
                    return false;
                  } else {
                    return byAge.doPrune(ageSec, searcher);
                  }
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
        if (writer == null) {
          break;
        }
      }
    }
  }

  /** Start the searcher pruning thread. */
  public void startSearcherPruningThread(CountDownLatch shutdownNow) {
    // nocommit make one thread in GlobalState
    if (searcherPruningThread == null) {
      searcherPruningThread = new SearcherPruningThread(shutdownNow);
      searcherPruningThread.setName("LuceneSearcherPruning-" + name);
      searcherPruningThread.start();
    }
  }

  /** Start this shard as standalone (not primary nor replica) */
  public synchronized void start() throws Exception {

    if (isStarted()) {
      throw new IllegalStateException("index \"" + name + "\" was already started");
    }

    boolean success = false;

    try {

      if (indexState.saveLoadState == null) {
        indexState.initSaveLoadState();
      }

      Path indexDirFile;
      if (rootDir == null) {
        indexDirFile = null;
      } else {
        indexDirFile = rootDir.resolve("index");
      }
      origIndexDir = indexState.df.open(indexDirFile);

      // nocommit don't allow RAMDir
      // nocommit remove NRTCachingDir too?
      if ((origIndexDir instanceof MMapDirectory) == false) {
        double maxMergeSizeMB =
            indexState.getDoubleSetting("nrtCachingDirectoryMaxMergeSizeMB", 5.0);
        double maxSizeMB = indexState.getDoubleSetting("nrtCachingDirectoryMaxSizeMB", 60.0);
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
        taxoDirFile = rootDir.resolve("taxonomy");
      }
      taxoDir = indexState.df.open(taxoDirFile);

      taxoSnapshots =
          new PersistentSnapshotDeletionPolicy(
              new KeepOnlyLastCommitDeletionPolicy(),
              taxoDir,
              IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

      taxoWriter =
          new DirectoryTaxonomyWriter(taxoDir, openMode) {
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
          new IndexWriter(
              indexDir, indexState.getIndexWriterConfig(openMode, origIndexDir, shardOrd));
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
              writer,
              true,
              new SearcherFactory() {
                @Override
                public IndexSearcher newSearcher(IndexReader r, IndexReader previousReader)
                    throws IOException {
                  IndexSearcher searcher = new MyIndexSearcher(r, searchExecutor);
                  searcher.setSimilarity(indexState.sim);
                  return searcher;
                }
              },
              taxoWriter);

      restartReopenThread();

      startSearcherPruningThread(indexState.globalState.shutdownNow);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(
            reopenThread, manager, writer, taxoWriter, slm, indexDir, taxoDir);
        writer = null;
      }
    }
  }

  /**
   * Start this index as primary, to NRT-replicate to replicas. primaryGen should be incremented
   * each time a new primary is promoted for a given index.
   */
  public synchronized void startPrimary(long primaryGen, Path dataPath) throws Exception {
    if (isStarted()) {
      throw new IllegalStateException("index \"" + name + "\" was already started");
    }

    // nocommit share code better w/ start and startReplica!

    boolean success = false;

    try {
      // we have backups and are not creating a new index
      // use that to load indexes and other state (registeredFields, settings)
      if (!doCreate && dataPath != null) {
        if (indexState.rootDir != null) {
          synchronized (this) {
            // copy downloaded data into rootDir
            indexState.restoreDir(dataPath, indexState.rootDir);
          }
          indexState.initSaveLoadState();
        }
      }

      if (indexState.saveLoadState == null) {
        indexState.initSaveLoadState();
      }

      Path indexDirFile;
      if (rootDir == null) {
        indexDirFile = null;
      } else {
        indexDirFile = rootDir.resolve("index");
      }
      origIndexDir = indexState.df.open(indexDirFile);

      if ((origIndexDir instanceof MMapDirectory) == false) {
        double maxMergeSizeMB =
            indexState.getDoubleSetting("nrtCachingDirectoryMaxMergeSizeMB", 5.0);
        double maxSizeMB = indexState.getDoubleSetting("nrtCachingDirectoryMaxSizeMB", 60.0);
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

      boolean verbose = indexState.getBooleanSetting("indexVerbose", false);

      writer =
          new IndexWriter(
              indexDir, indexState.getIndexWriterConfig(openMode, origIndexDir, shardOrd));
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

      HostPort hostPort =
          new HostPort(
              indexState.globalState.getHostName(), indexState.globalState.getReplicationPort());
      nrtPrimaryNode =
          new NRTPrimaryNode(
              indexState.name,
              hostPort,
              writer,
              0,
              primaryGen,
              -1,
              new SearcherFactory() {
                @Override
                public IndexSearcher newSearcher(IndexReader r, IndexReader previousReader)
                    throws IOException {
                  IndexSearcher searcher = new MyIndexSearcher(r, searchExecutor);
                  searcher.setSimilarity(indexState.sim);
                  return searcher;
                }
              },
              verbose ? System.out : new PrintStream(OutputStream.nullOutputStream()));

      // nocommit this isn't used?
      searcherManager =
          new NRTPrimaryNode.PrimaryNodeReferenceManager(
              nrtPrimaryNode,
              new SearcherFactory() {
                @Override
                public IndexSearcher newSearcher(IndexReader r, IndexReader previousReader)
                    throws IOException {
                  IndexSearcher searcher = new MyIndexSearcher(r, searchExecutor);
                  searcher.setSimilarity(indexState.sim);
                  return searcher;
                }
              });
      restartReopenThread();

      startSearcherPruningThread(indexState.globalState.shutdownNow);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(
            reopenThread, nrtPrimaryNode, writer, taxoWriter, slm, indexDir, taxoDir);
        writer = null;
      }
    }
  }

  private IndexReader.ClosedListener removeSSDVStates =
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
      SearcherTaxonomyManager.SearcherAndTaxonomy s, FieldDef fd) throws IOException {
    FacetsConfig.DimConfig dimConfig = indexState.facetsConfig.getDimConfig(fd.getName());
    IndexReader reader = s.searcher.getIndexReader();
    synchronized (ssdvStates) {
      Map<String, SortedSetDocValuesReaderState> readerSSDVStates = ssdvStates.get(reader);
      if (readerSSDVStates == null) {
        readerSSDVStates = new HashMap<>();
        ssdvStates.put(reader.getReaderCacheHelper().getKey(), readerSSDVStates);
        reader.getReaderCacheHelper().addClosedListener(removeSSDVStates);
      }

      SortedSetDocValuesReaderState ssdvState = readerSSDVStates.get(dimConfig.indexFieldName);
      if (ssdvState == null) {
        // TODO: maybe we should do this up front when reader is first opened instead
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
        // nocommit maybe we shouldn't make this a hard error
        // ... ie just return 0 facets
        if (ssdvState == null) {
          throw new IllegalArgumentException(
              String.format(
                  "field %s was properly registered with facet=sortedSetDocValues, however no documents were indexed as of this searcher"));
        }
        readerSSDVStates.put(dimConfig.indexFieldName, ssdvState);
      }

      return ssdvState;
    }
  }

  /* TODO: read remote state from s3 */
  @VisibleForTesting
  public static File remoteStateExists(Path basePath) {
    File[] primaryGenDirs = basePath.toFile().listFiles(File::isDirectory);
    File highestNumberedPrimaryGen = getHighestNumberedDir(primaryGenDirs);
    if (highestNumberedPrimaryGen == null) {
      return null;
    }

    File[] nrtVersions =
        basePath
            .resolve(Paths.get(highestNumberedPrimaryGen.getName()))
            .toFile()
            .listFiles(File::isDirectory);
    File highestNumberVerion = getHighestNumberedDir(nrtVersions);
    return highestNumberVerion;
  }

  @VisibleForTesting
  private static File getHighestNumberedDir(File[] directories) {
    if (directories.length == 0) {
      return null;
    }
    File latestDir = directories[0];
    int highestVersion = 0;
    for (File directory : directories) {
      int versionNum = Integer.valueOf(directory.getName());
      if (versionNum > highestVersion) {
        highestVersion = versionNum;
        latestDir = directory;
      }
    }
    return latestDir;
  }

  /** Start this index as replica, pulling NRT changes from the specified primary */
  public synchronized void startReplica(
      ReplicationServerClient primaryAddress, long primaryGen, Path dataPath) throws Exception {
    if (isStarted()) {
      throw new IllegalStateException("index \"" + name + "\" was already started");
    }
    // we have backups and are not creating a new index
    // use that to load indexes and other state (registeredFields, settings)
    if (!doCreate && dataPath != null) {
      if (indexState.rootDir != null) {
        synchronized (this) {
          // copy downloaded data into rootDir
          indexState.restoreDir(dataPath, indexState.rootDir);
        }
        indexState.initSaveLoadState();
      }
    }

    // nocommit share code better w/ start and startPrimary!
    boolean success = false;
    try {
      if (indexState.saveLoadState == null) {
        indexState.initSaveLoadState();
      }
      Path indexDirFile;
      if (rootDir == null) {
        indexDirFile = null;
      } else {
        indexDirFile = rootDir.resolve("index");
      }
      origIndexDir = indexState.df.open(indexDirFile);
      // nocommit don't allow RAMDir
      // nocommit remove NRTCachingDir too?
      if ((origIndexDir instanceof MMapDirectory) == false) {
        double maxMergeSizeMB =
            indexState.getDoubleSetting("nrtCachingDirectoryMaxMergeSizeMB", 5.0);
        double maxSizeMB = indexState.getDoubleSetting("nrtCachingDirectoryMaxSizeMB", 60.0);
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

      boolean verbose = indexState.getBooleanSetting("indexVerbose", false);

      HostPort hostPort =
          new HostPort(
              indexState.globalState.getHostName(), indexState.globalState.getReplicationPort());
      nrtReplicaNode =
          new NRTReplicaNode(
              indexState.name,
              primaryAddress,
              hostPort,
              REPLICA_ID,
              indexDir,
              new SearcherFactory() {
                @Override
                public IndexSearcher newSearcher(IndexReader r, IndexReader previousReader)
                    throws IOException {
                  IndexSearcher searcher = new MyIndexSearcher(r, searchExecutor);
                  searcher.setSimilarity(indexState.sim);
                  return searcher;
                }
              },
              verbose ? System.out : new PrintStream(OutputStream.nullOutputStream()),
              primaryGen);

      startSearcherPruningThread(indexState.globalState.shutdownNow);

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
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(
            reopenThread, nrtReplicaNode, writer, taxoWriter, slm, indexDir, taxoDir);
        writer = null;
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

  /** Context to hold state for a single indexing request. */
  public static class IndexingContext {

    /** How many chunks are still indexing. */
    public final Phaser inFlightChunks = new Phaser();

    /** How many documents were added. */
    public final AtomicInteger addCount = new AtomicInteger();

    /** Any indexing errors that occurred. */
    public final AtomicReference<Throwable> error = new AtomicReference<>();

    /** Sole constructor. */
    public IndexingContext() {}

    /** Only keeps the first error seen, and all bulk indexing stops after this. */
    public void setError(Throwable t) {
      // System.out.println("IndexingContext.setError:");
      // t.printStackTrace(System.out);
      error.compareAndSet(null, t);
    }

    /** Returns the first exception hit while indexing, or null */
    public Throwable getError() {
      return error.get();
    }
  }

  /** Job for a single block addDocuments call. */
  class AddDocumentsJob implements Callable<Long> {
    private final Term updateTerm;
    private final Iterable<Document> docs;
    private final IndexingContext ctx;

    // Position of this document in the bulk request:
    private final int index;

    /** Sole constructor. */
    public AddDocumentsJob(
        int index, Term updateTerm, Iterable<Document> docs, IndexingContext ctx) {
      this.updateTerm = updateTerm;
      this.docs = docs;
      this.ctx = ctx;
      this.index = index;
    }

    @Override
    public Long call() throws Exception {
      long gen = -1;
      try {
        Iterable<Document> justDocs;
        if (indexState.hasFacets()) {
          List<Document> justDocsList = new ArrayList<Document>();
          for (Document doc : docs) {
            // Translate any FacetFields:
            justDocsList.add(indexState.facetsConfig.build(taxoWriter, doc));
          }
          justDocs = justDocsList;
        } else {
          justDocs = docs;
        }

        // System.out.println(Thread.currentThread().getName() + ": add; " + docs);
        if (updateTerm == null) {
          gen = writer.addDocuments(justDocs);
        } else {
          gen = writer.updateDocuments(updateTerm, justDocs);
        }
      } catch (Exception e) {
        ctx.setError(new RuntimeException("error while indexing document " + index, e));
      } finally {
        ctx.addCount.incrementAndGet();
        // TODO: Semaphore to be acquired before submitting a job on its own thread
        // indexState.globalState.indexingJobsRunning.release();
      }

      return gen;
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
          shardState.indexState.globalState.getReplicaReplicationPortPingInterval();
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
                    shardState.indexState.name,
                    REPLICA_ID,
                    nrtReplicaNode.getHostPort().getHostName(),
                    nrtReplicaNode.getHostPort().getPort());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (StatusRuntimeException e) {
          logger.warn(
              String.format(
                  "Replica host: %s, binary port: %s cannot reach primary host: %s replication port: %s",
                  nrtReplicaNode.getHostPort().getHostName(),
                  nrtReplicaNode.getHostPort().getPort(),
                  nrtReplicaNode.getPrimaryAddress().getHost(),
                  nrtReplicaNode.getPrimaryAddress().getPort()));
        }
      }
    }

    @Override
    public void close() {
      exit = true;
    }
  }
}
