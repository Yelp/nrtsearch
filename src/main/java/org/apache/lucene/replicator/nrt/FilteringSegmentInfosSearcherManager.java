/*
 * Copyright 2023 Yelp Inc.
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
package org.apache.lucene.replicator.nrt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.StringHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of {@link SegmentInfosSearcherManager} which filters previous readers that are
 * incompatible during a refresh. Useful for replicas, when a primary restart may lead to segment
 * name reuse for un-committed data synced through nrt points.
 */
public class FilteringSegmentInfosSearcherManager extends SegmentInfosSearcherManager {
  private static final Logger logger =
      LoggerFactory.getLogger(FilteringSegmentInfosSearcherManager.class);
  private final Directory dir;
  private final Node node;
  private final AtomicInteger openReaderCount = new AtomicInteger();
  private final SearcherFactory searcherFactory;
  private long refreshedPrimaryGen = -1;
  private long currentPrimaryGen = -1;

  public FilteringSegmentInfosSearcherManager(
      Directory dir,
      Node node,
      ReferenceManager<IndexSearcher> mgr,
      long initialPrimaryGen,
      SearcherFactory searcherFactory)
      throws IOException {
    super(dir, node, ((SegmentInfosSearcherManager) mgr).getCurrentInfos(), searcherFactory);
    this.dir = dir;
    this.node = node;
    if (searcherFactory == null) {
      searcherFactory = new SearcherFactory();
    }
    this.searcherFactory = searcherFactory;
    // Seed refreshedPrimaryGen from the initial NRT point state so that the first NRT refresh
    // correctly detects a primary change. Without this, refreshedPrimaryGen stays at -1, which
    // suppresses the primaryChanged check and allows stale readers to be reused across a primary
    // restart that happens between the initial index download and the first NRT copy.
    if (initialPrimaryGen >= 0) {
      this.refreshedPrimaryGen = initialPrimaryGen;
    }
  }

  /**
   * Notify this manager of the primary generation for the NRT point about to be refreshed. Must be
   * called before the refresh is triggered (i.e., before {@link
   * SegmentInfosSearcherManager#setCurrentInfos} leads to a {@code maybeRefresh}). When the primary
   * generation changes, a stricter reader-compatibility check is applied for the next refresh to
   * handle the gen-reuse case (where a primary restart resets generation counters to values that
   * were previously used before the restart).
   *
   * @param primaryGen primary generation from the NRT copy state
   */
  public synchronized void setCurrentPrimaryGen(long primaryGen) {
    this.currentPrimaryGen = primaryGen;
  }

  @Override
  protected IndexSearcher refreshIfNeeded(IndexSearcher old) throws IOException {
    final SegmentInfos newInfos = getCurrentInfos();
    // Snapshot primaryGen state under lock so it is consistent within this refresh.
    final long localCurrentPrimaryGen;
    final long localRefreshedPrimaryGen;
    synchronized (this) {
      localCurrentPrimaryGen = currentPrimaryGen;
      localRefreshedPrimaryGen = refreshedPrimaryGen;
    }
    // All reader compatibility checks are only necessary on the first refresh after the primary
    // changes: segment name reuse, backward gen, and the gen-reuse case (where the generation
    // counter resets to a previously-used value after a restart) can only occur across a primary
    // restart boundary. We only pay this cost for the one refresh immediately after the primary
    // changes; subsequent refreshes resume normal core sharing.
    final boolean primaryChanged =
        localRefreshedPrimaryGen >= 0 && localCurrentPrimaryGen != localRefreshedPrimaryGen;

    List<LeafReader> subs;
    if (old == null) {
      subs = null;
    } else {
      List<LeafReaderContext> leaves = old.getIndexReader().leaves();
      // create map of segment name to reader ordinal
      final Map<String, Integer> oldReadersMap = new HashMap<>();
      for (int i = 0; i < leaves.size(); ++i) {
        final SegmentReader sr = (SegmentReader) leaves.get(i).reader();
        oldReadersMap.put(sr.getSegmentName(), i);
      }
      subs = new ArrayList<>();
      int filteredCount = 0;
      int reusedCount = 0;
      for (SegmentCommitInfo commitInfo : newInfos) {
        Integer oldReaderIndex = oldReadersMap.get(commitInfo.info.name);
        if (oldReaderIndex != null) {
          SegmentReader oldReader = (SegmentReader) leaves.get(oldReaderIndex).reader();
          if (primaryChanged) {
            // On the first refresh after a primary change, apply strict compatibility checks.
            // All three conditions below can only occur due to a primary restart, so we only
            // pay this cost once per primary change; subsequent refreshes resume normal core
            // sharing.
            if (!Arrays.equals(commitInfo.info.getId(), oldReader.getSegmentInfo().info.getId())) {
              // Segment name was reused for entirely different data after a primary restart.
              logger.info(
                  "Skipping incompatible old reader, name: "
                      + commitInfo.info.name
                      + ", old id: "
                      + StringHelper.idToString(oldReader.getSegmentInfo().info.getId())
                      + ", new id: "
                      + StringHelper.idToString(commitInfo.info.getId()));
              filteredCount++;
            } else if (!Arrays.equals(commitInfo.getId(), oldReader.getSegmentInfo().getId())) {
              // SegmentCommitInfo ID differs: this segment's commit state changed after the
              // primary restart. Force a fresh reader to avoid sharing a SegmentDocValues cache
              // that may hold stale producers from pre-restart generations (gen-reuse case: the
              // simple "gen < old gen" check cannot detect equal-but-different fieldInfosGen
              // values).
              logger.info(
                  "Skipping old reader after primary change, name: "
                      + commitInfo.info.name
                      + ", old commitInfo id: "
                      + StringHelper.idToString(oldReader.getSegmentInfo().getId())
                      + ", new commitInfo id: "
                      + StringHelper.idToString(commitInfo.getId())
                      + ", old primaryGen: "
                      + localRefreshedPrimaryGen
                      + ", new primaryGen: "
                      + localCurrentPrimaryGen);
              filteredCount++;
            } else if (commitInfo.getFieldInfosGen() < oldReader.getSegmentInfo().getFieldInfosGen()
                || commitInfo.getDelGen() < oldReader.getSegmentInfo().getDelGen()) {
              // Generation went backwards (e.g. primary restarted and lost uncommitted doc values
              // updates). Force a fresh reader with no shared core/segDocValues state to avoid
              // inconsistent doc values data.
              logger.info(
                  "Skipping old reader with backward generation, name: "
                      + commitInfo.info.name
                      + ", old fieldInfosGen: "
                      + oldReader.getSegmentInfo().getFieldInfosGen()
                      + ", new fieldInfosGen: "
                      + commitInfo.getFieldInfosGen()
                      + ", old delGen: "
                      + oldReader.getSegmentInfo().getDelGen()
                      + ", new delGen: "
                      + commitInfo.getDelGen());
              filteredCount++;
            } else {
              subs.add(oldReader);
              reusedCount++;
            }
          } else {
            subs.add(oldReader);
          }
        }
      }
      if (primaryChanged) {
        logger.info(
            "Primary generation changed ("
                + localRefreshedPrimaryGen
                + " -> "
                + localCurrentPrimaryGen
                + "), filtered "
                + filteredCount
                + " reader(s), reused "
                + reusedCount
                + " reader(s)");
      }
    }

    // Open a new reader, sharing any common segment readers with the old one:
    DirectoryReader r = StandardDirectoryReader.open(dir, newInfos, subs, null);
    addReaderClosedListenerFilter(r);
    node.message("refreshed to version=" + newInfos.getVersion() + " r=" + r);
    IndexReader oldReader = old != null ? old.getIndexReader() : null;
    IndexSearcher searcher = SearcherManager.getSearcher(searcherFactory, r, oldReader);
    // Record the primary gen for this completed refresh so the next refresh can detect changes.
    synchronized (this) {
      refreshedPrimaryGen = localCurrentPrimaryGen;
    }
    return searcher;
  }

  private void addReaderClosedListenerFilter(IndexReader r) {
    IndexReader.CacheHelper cacheHelper = r.getReaderCacheHelper();
    if (cacheHelper == null) {
      throw new IllegalStateException("StandardDirectoryReader must support caching");
    }
    openReaderCount.incrementAndGet();
    cacheHelper.addClosedListener(cacheKey -> onReaderClosedFilter());
  }

  /**
   * Tracks how many readers are still open, so that when we are closed, we can additionally wait
   * until all in-flight searchers are closed. This method must have a different name than the one
   * in the parent class, since the reference counts are maintained separately.
   */
  synchronized void onReaderClosedFilter() {
    if (openReaderCount.decrementAndGet() == 0) {
      notifyAll();
    }
  }
}
