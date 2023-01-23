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
import java.util.Collections;
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

  public FilteringSegmentInfosSearcherManager(
      Directory dir,
      Node node,
      ReferenceManager<IndexSearcher> mgr,
      SearcherFactory searcherFactory)
      throws IOException {
    super(dir, node, ((SegmentInfosSearcherManager) mgr).getCurrentInfos(), searcherFactory);
    this.dir = dir;
    this.node = node;
    if (searcherFactory == null) {
      searcherFactory = new SearcherFactory();
    }
    this.searcherFactory = searcherFactory;
  }

  @Override
  protected IndexSearcher refreshIfNeeded(IndexSearcher old) throws IOException {
    final SegmentInfos newInfos = getCurrentInfos();
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
      for (SegmentCommitInfo commitInfo : newInfos) {
        Integer oldReaderIndex = oldReadersMap.get(commitInfo.info.name);
        if (oldReaderIndex != null) {
          SegmentReader oldReader = (SegmentReader) leaves.get(oldReaderIndex).reader();
          // check if old reader is compatible with new segment data
          if (Arrays.equals(commitInfo.info.getId(), oldReader.getSegmentInfo().info.getId())) {
            subs.add(oldReader);
          } else {
            logger.info(
                "Skipping incompatible old reader, name: "
                    + commitInfo.info.name
                    + ", old id: "
                    + StringHelper.idToString(oldReader.getSegmentInfo().info.getId())
                    + ", new id: "
                    + StringHelper.idToString(commitInfo.info.getId()));
          }
        }
      }
    }

    // Open a new reader, sharing any common segment readers with the old one:
    DirectoryReader r = StandardDirectoryReader.open(dir, newInfos, subs, Collections.emptyMap());
    addReaderClosedListenerFilter(r);
    node.message("refreshed to version=" + newInfos.getVersion() + " r=" + r);
    IndexReader oldReader = old != null ? old.getIndexReader() : null;
    return SearcherManager.getSearcher(searcherFactory, r, oldReader);
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
