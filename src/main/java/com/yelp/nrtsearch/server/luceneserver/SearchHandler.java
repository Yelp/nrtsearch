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

import com.google.common.collect.Lists;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.SearchState;
import com.yelp.nrtsearch.server.grpc.TotalHits;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.facet.DrillSidewaysImpl;
import com.yelp.nrtsearch.server.luceneserver.facet.FacetTopDocs;
import com.yelp.nrtsearch.server.luceneserver.field.BooleanFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.DateTimeFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.ObjectFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.PolygonfieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.luceneserver.rescore.RescoreTask;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCutoffWrapper.CollectionTimeoutException;
import com.yelp.nrtsearch.server.luceneserver.search.SearchRequestProcessor;
import com.yelp.nrtsearch.server.luceneserver.search.SearcherResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchHandler implements Handler<SearchRequest, SearchResponse> {

  private static final Logger logger = LoggerFactory.getLogger(SearchHandler.class);
  private final ThreadPoolExecutor threadPoolExecutor;
  private final boolean warming;

  public SearchHandler(ThreadPoolExecutor threadPoolExecutor) {
    this(threadPoolExecutor, false);
  }

  /**
   * @param threadPoolExecutor Threadpool to execute a parallel search
   * @param warming set to true if we are warming the index right now
   */
  public SearchHandler(ThreadPoolExecutor threadPoolExecutor, boolean warming) {
    this.threadPoolExecutor = threadPoolExecutor;
    this.warming = warming;
  }

  @Override
  public SearchResponse handle(IndexState indexState, SearchRequest searchRequest)
      throws SearchHandlerException {
    ShardState shardState = indexState.getShard(0);

    // Index won't be started if we are currently warming
    if (!warming) {
      indexState.verifyStarted();
    }

    var diagnostics = SearchResponse.Diagnostics.newBuilder();

    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    SearchContext searchContext;
    try {
      s = getSearcherAndTaxonomy(searchRequest, shardState, diagnostics, threadPoolExecutor);

      ProfileResult.Builder profileResultBuilder = null;
      if (searchRequest.getProfile()) {
        profileResultBuilder = ProfileResult.newBuilder();
      }

      searchContext =
          SearchRequestProcessor.buildContextForRequest(
              searchRequest, indexState, shardState, s, profileResultBuilder);

      long searchStartTime = System.nanoTime();

      SearcherResult searcherResult;
      TopDocs hits;
      if (!searchRequest.getFacetsList().isEmpty()) {
        if (!(searchContext.getQuery() instanceof DrillDownQuery)) {
          throw new IllegalArgumentException("Can only use DrillSideways on DrillDownQuery");
        }
        DrillDownQuery ddq = (DrillDownQuery) searchContext.getQuery();

        List<FacetResult> grpcFacetResults = new ArrayList<>();
        DrillSideways drillS =
            new DrillSidewaysImpl(
                s.searcher,
                indexState.facetsConfig,
                s.taxonomyReader,
                searchRequest.getFacetsList(),
                s,
                shardState,
                searchContext.getQueryFields(),
                grpcFacetResults,
                threadPoolExecutor,
                diagnostics);
        DrillSideways.ConcurrentDrillSidewaysResult<SearcherResult> concurrentDrillSidewaysResult;
        try {
          concurrentDrillSidewaysResult =
              drillS.search(ddq, searchContext.getCollector().getWrappedManager());
        } catch (RuntimeException e) {
          // Searching with DrillSideways wraps exceptions in a few layers.
          // Try to find if this was caused by a timeout, if so, re-wrap
          // so that the top level exception is the same as when not using facets.
          CollectionTimeoutException timeoutException = findTimeoutException(e);
          if (timeoutException != null) {
            throw new CollectionTimeoutException(timeoutException.getMessage(), e);
          }
          throw e;
        }
        searcherResult = concurrentDrillSidewaysResult.collectorResult;
        hits = searcherResult.getTopDocs();
        searchContext.getResponseBuilder().addAllFacetResult(grpcFacetResults);
        searchContext
            .getResponseBuilder()
            .addAllFacetResult(
                FacetTopDocs.facetTopDocsSample(
                    hits, searchRequest.getFacetsList(), indexState, s.searcher, diagnostics));
      } else {
        searcherResult =
            s.searcher.search(
                searchContext.getQuery(), searchContext.getCollector().getWrappedManager());
        hits = searcherResult.getTopDocs();
      }

      // add results from any extra collectors
      searchContext
          .getResponseBuilder()
          .putAllCollectorResults(searcherResult.getCollectorResults());

      searchContext.getResponseBuilder().setHitTimeout(searchContext.getCollector().hadTimeout());

      diagnostics.setFirstPassSearchTimeMs(((System.nanoTime() - searchStartTime) / 1000000.0));

      // add detailed timing metrics for query execution
      if (profileResultBuilder != null) {
        searchContext.getCollector().maybeAddProfiling(profileResultBuilder);
      }

      long rescoreStartTime = System.nanoTime();

      if (!searchContext.getRescorers().isEmpty()) {
        for (RescoreTask rescorer : searchContext.getRescorers()) {
          hits = rescorer.rescore(s.searcher, hits);
        }
        diagnostics.setRescoreTimeMs(((System.nanoTime() - rescoreStartTime) / 1000000.0));
      }

      long t0 = System.nanoTime();

      hits = getHitsFromOffset(hits, searchContext.getStartHit(), searchContext.getTopHits());

      // create Hit.Builder for each hit, and populate with lucene doc id and ranking info
      setResponseHits(searchContext, hits);

      // fill all other needed fields into each Hit.Builder
      List<Hit.Builder> hitBuilders =
          new ArrayList<>(searchContext.getResponseBuilder().getHitsBuilderList());
      hitBuilders.sort(Comparator.comparing(Hit.Builder::getLuceneDocId));
      List<LeafReaderContext> leaves = s.searcher.getIndexReader().leaves();

      List<LeafReaderContext> hitIdToLeaves = new ArrayList<>();
      for (int hitIndex = 0; hitIndex < hitBuilders.size(); ++hitIndex) {
        var hitResponse = hitBuilders.get(hitIndex);
        LeafReaderContext leaf =
            leaves.get(ReaderUtil.subIndex(hitResponse.getLuceneDocId(), leaves));
        hitIdToLeaves.add(hitIndex, leaf);
      }
      List<String> fields = new ArrayList<>(searchContext.getRetrieveFields().keySet());
      int fetch_thread_pool_size = indexState.getThreadPoolConfiguration().getMaxFetchThreads();
      int min_parallel_fetch_num_fields =
          indexState.getThreadPoolConfiguration().getMinParallelFetchNumFields();
      int min_parallel_fetch_num_hits =
          indexState.getThreadPoolConfiguration().getMinParallelFetchNumHits();
      // only parallel load data when request retrieve large number of fields for large number of
      // hits
      if (fetch_thread_pool_size > 1
          && fields.size() > min_parallel_fetch_num_fields
          && hitBuilders.size() > min_parallel_fetch_num_hits) {
        // parallelism is min of fetchThreadPoolSize and fields.size() / MIN_PARALLEL_NUM_FIELDS
        // round up
        int parallelism =
            Math.min(
                fetch_thread_pool_size,
                (fields.size() + min_parallel_fetch_num_fields - 1)
                    / min_parallel_fetch_num_fields);
        List<List<String>> fieldsChunks =
            Lists.partition(fields, (fields.size() + parallelism - 1) / parallelism);

        List<Future<List<Map<String, CompositeFieldValue>>>> futures = new ArrayList<>();
        // Only parallel by fields here, which should work well for doc values and virtual fields
        // For row based stored fields, we should do it by hit id.
        // Stored fields are not widely used for NRTSearch (not recommended for memory usage)
        for (List<String> fieldsChunk : fieldsChunks) {
          futures.add(
              indexState
                  .getFetchThreadPoolExecutor()
                  .submit(
                      new FillFieldsTask(
                          indexState,
                          s.searcher,
                          hitIdToLeaves,
                          hitBuilders,
                          fieldsChunk,
                          searchContext)));
        }
        for (Future<List<Map<String, CompositeFieldValue>>> future : futures) {
          List<Map<String, CompositeFieldValue>> values = future.get();
          for (int hitIndex = 0; hitIndex < hitBuilders.size(); ++hitIndex) {
            var hitResponse = hitBuilders.get(hitIndex);
            hitResponse.putAllFields(values.get(hitIndex));
          }
        }
      } else {
        List<Map<String, CompositeFieldValue>> values =
            new FillFieldsTask(
                    indexState, s.searcher, hitIdToLeaves, hitBuilders, fields, searchContext)
                .call();
        for (int hitIndex = 0; hitIndex < hitBuilders.size(); ++hitIndex) {
          var hitResponse = hitBuilders.get(hitIndex);
          hitResponse.putAllFields(values.get(hitIndex));
        }
      }

      // TODO: support highlight fields
      Object highlighter = null;
      for (int hitIndex = 0; hitIndex < hitBuilders.size(); ++hitIndex) {
        // TODO: get real highlights later
        Map<String, Object[]> highlights = Collections.emptyMap();

        if (highlights != null) {
          for (Map.Entry<String, Object[]> ent : highlights.entrySet()) {
            Object v = ent.getValue()[hitIndex];
            if (v != null) {
              // FIXME: not sure this is serializable to string?
              CompositeFieldValue value =
                  CompositeFieldValue.newBuilder()
                      .addFieldValue(FieldValue.newBuilder().setTextValue(v.toString()).build())
                      .build();
              hitBuilders.get(hitIndex).putFields(ent.getKey(), value);
            }
          }
        }
      }

      for (int hitIndex = 0; hitIndex < hitBuilders.size(); ++hitIndex) {
        var hitResponse = hitBuilders.get(hitIndex);
        LeafReaderContext leaf = hitIdToLeaves.get(hitIndex);
        searchContext.getFetchTasks().processHit(searchContext, leaf, hitResponse);
      }

      searchContext
          .getFetchTasks()
          .processAllHits(searchContext, searchContext.getResponseBuilder().getHitsBuilderList());

      SearchState.Builder searchState = SearchState.newBuilder();
      searchContext.getResponseBuilder().setSearchState(searchState);
      searchState.setTimestamp(searchContext.getTimestampSec());

      // Record searcher version that handled this request:
      searchState.setSearcherVersion(((DirectoryReader) s.searcher.getIndexReader()).getVersion());

      // Fill in lastDoc for searchAfter:
      if (hits.scoreDocs.length != 0) {
        ScoreDoc lastHit = hits.scoreDocs[hits.scoreDocs.length - 1];
        searchState.setLastDocId(lastHit.doc);
        searchContext.getCollector().fillLastHit(searchState, lastHit);
      }

      diagnostics.setGetFieldsTimeMs(((System.nanoTime() - t0) / 1000000.0));
      searchContext.getResponseBuilder().setDiagnostics(diagnostics);

      if (profileResultBuilder != null) {
        searchContext.getResponseBuilder().setProfileResult(profileResultBuilder);
      }
    } catch (IOException | InterruptedException | ExecutionException e) {
      logger.warn(e.getMessage(), e);
      throw new SearchHandlerException(e);
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
        throw new SearchHandlerException(e);
      }
    }

    // Add searchRequest to warmer if needed
    try {
      if (!warming && indexState.getWarmer() != null) {
        indexState.getWarmer().addSearchRequest(searchRequest);
      }
    } catch (Exception e) {
      logger.error("Unable to add warming query", e);
    }

    return searchContext.getResponseBuilder().build();
  }

  /**
   * Given all the top documents, produce a slice of the documents starting from a start offset and
   * going up to the query needed maximum hits. There may be more top docs than the topHits limit,
   * if top docs sampling facets are used.
   *
   * @param hits all hits
   * @param startHit offset into top docs
   * @param topHits maximum number of hits needed for search response
   * @return slice of hits starting at given offset, or empty slice if there are less than startHit
   *     docs
   */
  private static TopDocs getHitsFromOffset(TopDocs hits, int startHit, int topHits) {
    int retrieveHits = Math.min(topHits, hits.scoreDocs.length);
    if (startHit != 0 || retrieveHits != hits.scoreDocs.length) {
      // Slice:
      int count = Math.max(0, retrieveHits - startHit);
      ScoreDoc[] newScoreDocs = new ScoreDoc[count];
      if (count > 0) {
        System.arraycopy(hits.scoreDocs, startHit, newScoreDocs, 0, count);
      }
      return new TopDocs(hits.totalHits, newScoreDocs);
    }
    return hits;
  }

  /**
   * Add {@link com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder}s to the context {@link
   * SearchResponse.Builder} for each of the query hits. Populate the builders with the lucene doc
   * id and ranking info.
   *
   * @param context search context
   * @param hits hits from query
   */
  private static void setResponseHits(SearchContext context, TopDocs hits) {
    TotalHits totalHits =
        TotalHits.newBuilder()
            .setRelation(TotalHits.Relation.valueOf(hits.totalHits.relation.name()))
            .setValue(hits.totalHits.value)
            .build();
    context.getResponseBuilder().setTotalHits(totalHits);
    for (int hitIndex = 0; hitIndex < hits.scoreDocs.length; hitIndex++) {
      var hitResponse = context.getResponseBuilder().addHitsBuilder();
      ScoreDoc hit = hits.scoreDocs[hitIndex];
      hitResponse.setLuceneDocId(hit.doc);
      context.getCollector().fillHitRanking(hitResponse, hit);
    }
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
              new MyIndexSearcher(
                  r,
                  new MyIndexSearcher.ExecutorWithParams(
                      threadPoolExecutor,
                      state.indexState.getSliceMaxDocs(),
                      state.indexState.getSliceMaxSegments(),
                      state.indexState.getVirtualShards())),
              s.taxonomyReader);
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

  public static class FillFieldsTask implements Callable<List<Map<String, CompositeFieldValue>>> {

    private IndexState state;
    private IndexSearcher s;
    private List<LeafReaderContext> hitIdToleaves;
    private List<Hit.Builder> hitBuilders;
    private List<String> fields;
    private SearchContext searchContext;

    public FillFieldsTask(
        IndexState indexState,
        IndexSearcher indexSearcher,
        List<LeafReaderContext> hitIdToleaves,
        List<Hit.Builder> hitBuilders,
        List<String> fields,
        SearchContext searchContext) {
      this.state = indexState;
      this.s = indexSearcher;
      this.fields = fields;
      this.searchContext = searchContext;
      this.hitBuilders = hitBuilders;
      this.hitIdToleaves = hitIdToleaves;
    }

    private List<Map<String, CompositeFieldValue>> fillFields(
        IndexState state,
        IndexSearcher s,
        List<Hit.Builder> hitBuilders,
        List<String> fields,
        SearchContext searchContext)
        throws IOException {
      List<Map<String, CompositeFieldValue>> values = new ArrayList<>();
      for (int hitIndex = 0; hitIndex < hitBuilders.size(); ++hitIndex) {
        values.add(new HashMap<>());
      }
      for (String field : fields) {
        for (int hitIndex = 0; hitIndex < hitBuilders.size(); ++hitIndex) {
          var hitResponse = hitBuilders.get(hitIndex);
          LeafReaderContext leaf = hitIdToleaves.get(hitIndex);
          CompositeFieldValue v =
              getFieldForHit(state, s, hitResponse, leaf, field, searchContext.getRetrieveFields());
          values.get(hitIndex).put(field, v);
        }
      }
      return values;
    }

    /**
     * retrieve one field (some hilited) for one hit:
     *
     * @return
     */
    private CompositeFieldValue getFieldForHit(
        IndexState state,
        IndexSearcher s,
        Hit.Builder hit,
        LeafReaderContext leaf,
        String field,
        Map<String, FieldDef> dynamicFields)
        throws IOException {
      assert field != null;
      CompositeFieldValue.Builder compositeFieldValue = CompositeFieldValue.newBuilder();
      FieldDef fd = dynamicFields.get(field);

      // TODO: get highlighted fields as well
      // Map<String,Object> doc = highlighter.getDocument(state, s, hit.doc);
      Map<String, Object> doc = new HashMap<>();
      boolean docIdAdvanced = false;

      // We detect invalid field above:
      assert fd != null;

      if (fd instanceof VirtualFieldDef) {
        VirtualFieldDef virtualFieldDef = (VirtualFieldDef) fd;

        int docID = hit.getLuceneDocId() - leaf.docBase;

        assert !Double.isNaN(hit.getScore()) || !virtualFieldDef.getValuesSource().needsScores();
        DoubleValues scoreValue =
            new DoubleValues() {
              @Override
              public double doubleValue() throws IOException {
                return hit.getScore();
              }

              @Override
              public boolean advanceExact(int doc) throws IOException {
                return !Double.isNaN(hit.getScore());
              }
            };
        DoubleValues doubleValues = virtualFieldDef.getValuesSource().getValues(leaf, scoreValue);
        doubleValues.advanceExact(docID);
        compositeFieldValue.addFieldValue(
            FieldValue.newBuilder().setDoubleValue(doubleValues.doubleValue()));
      } else if (fd instanceof IndexableFieldDef && ((IndexableFieldDef) fd).hasDocValues()) {
        int docID = hit.getLuceneDocId() - leaf.docBase;
        // it may be possible to cache this if there are multiple hits in the same segment
        LoadedDocValues<?> docValues = ((IndexableFieldDef) fd).getDocValues(leaf);
        docValues.setDocId(docID);
        for (int i = 0; i < docValues.size(); ++i) {
          compositeFieldValue.addFieldValue(docValues.toFieldValue(i));
        }
      }
      // retrieve stored fields
      else if (fd instanceof IndexableFieldDef && ((IndexableFieldDef) fd).isStored()) {
        String[] values = ((IndexableFieldDef) fd).getStored(s.doc(hit.getLuceneDocId()));
        for (String fieldValue : values) {
          if (fd instanceof ObjectFieldDef || fd instanceof PolygonfieldDef) {
            Struct.Builder builder = Struct.newBuilder();
            JsonFormat.parser().merge(fieldValue, builder);
            compositeFieldValue.addFieldValue(FieldValue.newBuilder().setStructValue(builder));
          } else {
            compositeFieldValue.addFieldValue(FieldValue.newBuilder().setTextValue(fieldValue));
          }
        }
      } else {
        Object v = doc.get(field); // FIXME: doc is never updated, not sure if this is correct
        if (v != null) {
          if (fd instanceof IndexableFieldDef && !((IndexableFieldDef) fd).isMultiValue()) {
            compositeFieldValue.addFieldValue(convertType(fd, v));
          } else {
            if (!(v instanceof List)) {
              // FIXME: not sure this is serializable to string?
              compositeFieldValue.addFieldValue(convertType(fd, v));
            } else {
              for (Object o : (List<Object>) v) {
                // FIXME: not sure this is serializable to string?
                compositeFieldValue.addFieldValue(convertType(fd, o));
              }
            }
          }
        }
      }

      return compositeFieldValue.build();
    }

    @Override
    public List<Map<String, CompositeFieldValue>> call() throws IOException {
      return fillFields(state, s, hitBuilders, fields, searchContext);
    }
  }

  private static FieldValue convertType(FieldDef fd, Object o) {
    var fieldValue = FieldValue.newBuilder();
    if (fd instanceof BooleanFieldDef) {
      if ((Integer) o == 1) {
        fieldValue.setBooleanValue(Boolean.TRUE);
      } else {
        assert (Integer) o == 0;
        fieldValue.setBooleanValue(Boolean.FALSE);
      }
    } else if (fd instanceof DateTimeFieldDef) {
      fieldValue.setTextValue(msecToDateString(((DateTimeFieldDef) fd), ((Number) o).longValue()));
    } else {
      throw new IllegalArgumentException("Unable to convert object: " + o);
    }

    return fieldValue.build();
  }

  private static String msecToDateString(DateTimeFieldDef fd, long value) {
    // nocommit use CTL to reuse these?
    return fd.getDateTimeParser().parser.format(new Date(value));
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

  /**
   * Find an instance of {@link CollectionTimeoutException} in the cause path of an exception.
   *
   * @return found exception instance or null
   */
  private static CollectionTimeoutException findTimeoutException(Throwable e) {
    if (e instanceof CollectionTimeoutException) {
      return (CollectionTimeoutException) e;
    }
    if (e.getCause() != null) {
      return findTimeoutException(e.getCause());
    }
    return null;
  }
}
