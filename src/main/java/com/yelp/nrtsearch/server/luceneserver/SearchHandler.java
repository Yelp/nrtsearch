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

import com.google.gson.Gson;
import com.google.protobuf.Struct;
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
import com.yelp.nrtsearch.server.utils.StructJsonUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private final ThreadPoolExecutor threadPoolExecutor;
  Logger logger = LoggerFactory.getLogger(RegisterFieldsHandler.class);

  private final Gson gson = new Gson();

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
    SearchContext searchContext;
    try {
      s = getSearcherAndTaxonomy(searchRequest, shardState, diagnostics, threadPoolExecutor);

      searchContext =
          SearchRequestProcessor.buildContextForRequest(
              searchRequest, indexState, shardState, s, diagnostics);

      ProfileResult.Builder profileResultBuilder = null;
      if (searchRequest.getProfile()) {
        profileResultBuilder = ProfileResult.newBuilder();
      }

      long searchStartTime = System.nanoTime();

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
        DrillSideways.ConcurrentDrillSidewaysResult<? extends TopDocs>
            concurrentDrillSidewaysResult;
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
        hits = concurrentDrillSidewaysResult.collectorResult;
        searchContext.getResponseBuilder().addAllFacetResult(grpcFacetResults);
        searchContext
            .getResponseBuilder()
            .addAllFacetResult(
                FacetTopDocs.facetTopDocsSample(
                    hits, searchRequest.getFacetsList(), indexState, s.searcher, diagnostics));
      } else {
        hits =
            s.searcher.search(
                searchContext.getQuery(), searchContext.getCollector().getWrappedManager());
      }

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
      List<Hit.Builder> hitBuilders = searchContext.getResponseBuilder().getHitsBuilderList();
      List<LeafReaderContext> leaves = s.searcher.getIndexReader().leaves();

      Map<LeafReaderContext, List<Hit.Builder>> leafToHits = new HashMap<>();

      for (int hitIndex = 0; hitIndex < hitBuilders.size(); ++hitIndex) {
        var hitResponse = hitBuilders.get(hitIndex);

        LeafReaderContext leaf =
            leaves.get(ReaderUtil.subIndex(hitResponse.getLuceneDocId(), leaves));
        List<Hit.Builder> hitsForLeaf =
            leafToHits.computeIfAbsent(leaf, leafReaderContext -> new ArrayList<>());
        hitsForLeaf.add(hitResponse);

        if (!searchContext.getRetrieveFields().isEmpty()) {
          var fieldValueMap =
              fillFields(
                  indexState,
                  null,
                  s.searcher,
                  hitResponse,
                  leaf,
                  searchContext.getRetrieveFields().keySet(),
                  Collections.emptyMap(),
                  hitIndex,
                  searchContext.getRetrieveFields());
          hitResponse.putAllFields(fieldValueMap);
        }
      }

      for (Map.Entry<LeafReaderContext, List<Hit.Builder>> entry : leafToHits.entrySet()) {
        LeafReaderContext leaf = entry.getKey();
        List<Hit.Builder> hitsForLeaf = entry.getValue();
        hitsForLeaf.sort(Comparator.comparing(Hit.Builder::getLuceneDocId));
        for (Hit.Builder hit : hitsForLeaf) {
          searchContext.getFetchTasks().processHit(searchContext, leaf, hit);
        }
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
    } catch (IOException | InterruptedException e) {
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

  /**
   * Fills in the returned fields (some hilited) for one hit:
   *
   * @return
   */
  private Map<String, CompositeFieldValue> fillFields(
      IndexState state,
      Object highlighter,
      IndexSearcher s,
      Hit.Builder hit,
      LeafReaderContext leaf,
      Set<String> fields,
      Map<String, Object[]> highlights,
      int hiliteHitIndex,
      Map<String, FieldDef> dynamicFields)
      throws IOException {
    Map<String, CompositeFieldValue> fieldValueMap = new HashMap<>();
    if (fields != null) {
      // Add requested stored fields (no highlighting):

      // even if they were not stored ...
      // TODO: get highlighted fields as well
      // Map<String,Object> doc = highlighter.getDocument(state, s, hit.doc);
      Map<String, Object> doc = new HashMap<>();
      boolean docIdAdvanced = false;
      for (String name : fields) {
        CompositeFieldValue.Builder compositeFieldValue = CompositeFieldValue.newBuilder();
        FieldDef fd = dynamicFields.get(name);

        // We detect invalid field above:
        assert fd != null;

        // retrieve from doc values
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
              Map<String, Object> map = gson.fromJson(fieldValue, Map.class);
              Struct struct = StructJsonUtils.convertMapToStruct(map);
              compositeFieldValue.addFieldValue(FieldValue.newBuilder().setStructValue(struct));
            } else {
              compositeFieldValue.addFieldValue(FieldValue.newBuilder().setTextValue(fieldValue));
            }
          }
        } else {
          Object v = doc.get(name); // FIXME: doc is never updated, not sure if this is correct
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

        fieldValueMap.put(name, compositeFieldValue.build());
      }
    }

    if (highlights != null) {
      for (Map.Entry<String, Object[]> ent : highlights.entrySet()) {
        Object v = ent.getValue()[hiliteHitIndex];
        if (v != null) {
          // FIXME: not sure this is serializable to string?
          CompositeFieldValue value =
              CompositeFieldValue.newBuilder()
                  .addFieldValue(FieldValue.newBuilder().setTextValue(v.toString()).build())
                  .build();
          fieldValueMap.put(ent.getKey(), value);
        }
      }
    }

    return fieldValueMap;
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
