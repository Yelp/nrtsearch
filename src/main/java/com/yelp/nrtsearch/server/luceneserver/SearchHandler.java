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

import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.SearchExecutor;
import com.yelp.nrtsearch.server.luceneserver.search.SearchRequestProcessor;
import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchHandler implements Handler<SearchRequest, SearchResponse> {

  private final ThreadPoolExecutor threadPoolExecutor;
  Logger logger = LoggerFactory.getLogger(RegisterFieldsHandler.class);

  public SearchHandler(ThreadPoolExecutor threadPoolExecutor) {
    this.threadPoolExecutor = threadPoolExecutor;
  }

  @Override
  public SearchResponse handle(IndexState indexState, SearchRequest searchRequest)
      throws SearchHandlerException {
    ShardState shardState = indexState.getShard(0);
    indexState.verifyStarted();

    var diagnostics = SearchResponse.Diagnostics.newBuilder();
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    SearchResponse.Builder searchResponse;
    try {
      s = getSearcherAndTaxonomy(searchRequest, shardState, diagnostics, threadPoolExecutor);

      SearchContext context =
          SearchRequestProcessor.buildContextForRequest(
              searchRequest, indexState, shardState, s, diagnostics);
      searchResponse = SearchExecutor.doQueryPhase(context);
      SearchExecutor.doFetchPhase(context, searchResponse);
    } catch (IOException | InterruptedException e) {
      throw new SearchHandler.SearchHandlerException(e);
    } finally {
      // NOTE: this is a little iffy, because we may not
      // have obtained this searcher from the NRTManager
      // (i.e. sometimes we pulled from
      // SearcherLifetimeManager, other times (if
      // snapshot was specified) we opened ourselves,
      // but under-the-hood all these methods just call
      // s.getIndexReader().decRef(), which is what release
      // does:
      try {
        if (s != null) {
          shardState.release(s);
        }
      } catch (IOException e) {
        logger.warn("Failed to release searcher reference previously acquired by acquire()", e);
        throw new SearchHandler.SearchHandlerException(e);
      }
    }

    return searchResponse.build();
  }

  /**
   * Returns the requested searcher + taxoReader, either by indexGen, snapshot, version or just the
   * current (latest) one.
   */
  public static SearcherTaxonomyManager.SearcherAndTaxonomy getSearcherAndTaxonomy(
      SearchRequest searchRequest,
      ShardState state,
      SearchResponse.Diagnostics.Builder diagnostics,
      ThreadPoolExecutor threadPoolExecutor)
      throws InterruptedException, IOException {
    Logger logger = LoggerFactory.getLogger(SearcherTaxonomyManager.SearcherAndTaxonomy.class);
    // TODO: Figure out which searcher to use:
    // final long searcherVersion; e.g. searcher.getLong("version")
    // final IndexState.Gens searcherSnapshot; e.g. searcher.getLong("indexGen")
    // Currently we only use the current(latest) searcher
    SearcherTaxonomyManager.SearcherAndTaxonomy s;

    SearchRequest.SearcherCase searchCase = searchRequest.getSearcherCase();
    long version;
    IndexState.Gens snapshot;

    if (searchCase.equals(SearchRequest.SearcherCase.VERSION)) {
      // Searcher is identified by a version, returned by
      // a prior search or by a refresh.  Apps use this when
      // the user does a follow-on search (next page, drill
      // down, etc.), or to ensure changes from a refresh
      // or NRT replication point are reflected:
      version = searchRequest.getVersion();
      snapshot = null;
      // nocommit need to generify this so we can pull
      // TaxoReader too:
      IndexSearcher priorSearcher = state.slm.acquire(version);
      if (priorSearcher == null) {
        if (snapshot != null) {
          // First time this snapshot is being searched
          // against since this server started, or the call
          // to createSnapshot didn't specify
          // openSearcher=true; now open the reader:
          s = openSnapshotReader(state, snapshot, diagnostics, threadPoolExecutor);
        } else {
          SearcherTaxonomyManager.SearcherAndTaxonomy current = state.acquire();
          long currentVersion = ((DirectoryReader) current.searcher.getIndexReader()).getVersion();
          if (currentVersion == version) {
            s = current;
          } else if (version > currentVersion) {
            logger.info(
                "SearchHandler: now await version="
                    + version
                    + " vs currentVersion="
                    + currentVersion);

            // TODO: should we have some timeout here? if user passes bogus future version, we hang
            // forever:

            // user is asking for search version beyond what we are currently searching ... wait for
            // us to refresh to it:

            state.release(current);

            // TODO: Use FutureTask<SearcherAndTaxonomy> here?

            // nocommit: do this in an async way instead!  this task should be parked somewhere and
            // resumed once refresh runs and exposes
            // the requested version, instead of blocking the current search thread
            Lock lock = new ReentrantLock();
            Condition cond = lock.newCondition();
            ReferenceManager.RefreshListener listener =
                new ReferenceManager.RefreshListener() {
                  @Override
                  public void beforeRefresh() {}

                  @Override
                  public void afterRefresh(boolean didRefresh) throws IOException {
                    SearcherTaxonomyManager.SearcherAndTaxonomy current = state.acquire();
                    logger.info(
                        "SearchHandler: refresh completed newVersion="
                            + ((DirectoryReader) current.searcher.getIndexReader()).getVersion());
                    try {
                      if (((DirectoryReader) current.searcher.getIndexReader()).getVersion()
                          >= version) {
                        lock.lock();
                        try {
                          logger.info("SearchHandler: now signal new version");
                          cond.signal();
                        } finally {
                          lock.unlock();
                        }
                      }
                    } finally {
                      state.release(current);
                    }
                  }
                };
            state.addRefreshListener(listener);
            lock.lock();
            try {
              current = state.acquire();
              if (((DirectoryReader) current.searcher.getIndexReader()).getVersion() < version) {
                // still not there yet
                state.release(current);
                cond.await();
                current = state.acquire();
                logger.info(
                    "SearchHandler: await released,  current version "
                        + ((DirectoryReader) current.searcher.getIndexReader()).getVersion()
                        + " required minimum version "
                        + version);
                assert ((DirectoryReader) current.searcher.getIndexReader()).getVersion()
                    >= version;
              }
              s = current;
            } finally {
              lock.unlock();
              state.removeRefreshListener(listener);
            }
          } else {
            // Specific searcher version was requested,
            // but this searcher has timed out.  App
            // should present a "your session expired" to
            // user:
            throw new RuntimeException(
                "searcher: This searcher has expired version="
                    + version
                    + " vs currentVersion="
                    + currentVersion);
          }
        }
      } else {
        // nocommit messy ... we pull an old searcher
        // but the latest taxoReader ... necessary
        // because SLM can't take taxo reader yet:
        SearcherTaxonomyManager.SearcherAndTaxonomy s2 = state.acquire();
        s = new SearcherTaxonomyManager.SearcherAndTaxonomy(priorSearcher, s2.taxonomyReader);
        s2.searcher.getIndexReader().decRef();
      }
    } else if (searchCase.equals((SearchRequest.SearcherCase.INDEXGEN))) {
      // Searcher is identified by an indexGen, returned
      // from a previous indexing operation,
      // e.g. addDocument.  Apps use this then they want
      // to ensure a specific indexing change is visible:
      long t0 = System.nanoTime();
      long gen = searchRequest.getIndexGen();
      if (gen > state.writer.getMaxCompletedSequenceNumber()) {
        throw new RuntimeException(
            "indexGen: requested indexGen ("
                + gen
                + ") is beyond the current maximum generation ("
                + state.writer.getMaxCompletedSequenceNumber()
                + ")");
      }
      state.waitForGeneration(gen);
      if (diagnostics != null) {
        diagnostics.setNrtWaitTimeMs((System.nanoTime() - t0) / 1000000.0);
      }
      s = state.acquire();
      state.slm.record(s.searcher);
    } else if (searchCase.equals(SearchRequest.SearcherCase.SEARCHER_NOT_SET)) {
      // Request didn't specify any specific searcher;
      // just use the current (latest) searcher:
      s = state.acquire();
      state.slm.record(s.searcher);
    } else {
      throw new UnsupportedOperationException(searchCase.name() + " is not yet supported ");
    }

    return s;
  }

  /** Returns a ref. */
  private static SearcherTaxonomyManager.SearcherAndTaxonomy openSnapshotReader(
      ShardState state,
      IndexState.Gens snapshot,
      SearchResponse.Diagnostics.Builder diagnostics,
      ThreadPoolExecutor threadPoolExecutor)
      throws IOException {
    // TODO: this "reverse-NRT" is ridiculous: we acquire
    // the latest reader, and from that do a reopen to an
    // older snapshot ... this is inefficient if multiple
    // snaphots share older segments that the latest reader
    // does not share ... Lucene needs a reader pool
    // somehow:
    SearcherTaxonomyManager.SearcherAndTaxonomy s = state.acquire();
    try {
      // This returns a new reference to us, which
      // is decRef'd in the finally clause after
      // search is done:
      long t0 = System.nanoTime();

      // Returns a ref, which we return to caller:
      IndexReader r =
          DirectoryReader.openIfChanged(
              (DirectoryReader) s.searcher.getIndexReader(),
              state.snapshots.getIndexCommit(snapshot.indexGen));

      // Ref that we return to caller
      s.taxonomyReader.incRef();

      SearcherTaxonomyManager.SearcherAndTaxonomy result =
          new SearcherTaxonomyManager.SearcherAndTaxonomy(
              new MyIndexSearcher(r, threadPoolExecutor), s.taxonomyReader);
      state.slm.record(result.searcher);
      long t1 = System.nanoTime();
      if (diagnostics != null) {
        diagnostics.setNewSnapshotSearcherOpenMs(((t1 - t0) / 1000000.0));
      }
      return result;
    } finally {
      state.release(s);
    }
  }

  public static class SearchHandlerException extends HandlerException {

    public SearchHandlerException(Throwable err) {
      super(err);
    }

    public SearchHandlerException(String message) {
      super(message);
    }

    public SearchHandlerException(String message, Throwable err) {
      super(message, err);
    }
  }
}
