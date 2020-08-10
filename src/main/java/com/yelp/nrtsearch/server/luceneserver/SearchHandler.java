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

import com.google.common.collect.Maps;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.SearchState;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.grpc.TotalHits;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.facet.DrillSidewaysImpl;
import com.yelp.nrtsearch.server.luceneserver.field.BooleanFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.DateTimeFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.TextBaseFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Sortable;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptParamsTransformer;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import java.io.IOException;
import java.text.BreakIterator;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LargeNumHitsTopDocsCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.grouping.AllGroupsCollector;
import org.apache.lucene.search.grouping.FirstPassGroupingCollector;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchHandler implements Handler<SearchRequest, SearchResponse> {

  private final ThreadPoolExecutor threadPoolExecutor;
  Logger logger = LoggerFactory.getLogger(RegisterFieldsHandler.class);
  /**
   * By default we count hits accurately up to 1000. This makes sure that we don't spend most time
   * on computing hit counts
   */
  private static final int TOTAL_HITS_THRESHOLD = 1000;

  private static final QueryNodeMapper QUERY_NODE_MAPPER = new QueryNodeMapper();

  public SearchHandler(ThreadPoolExecutor threadPoolExecutor) {
    this.threadPoolExecutor = threadPoolExecutor;
  }

  @Override
  public SearchResponse handle(IndexState indexState, SearchRequest searchRequest)
      throws SearchHandlerException {
    ShardState shardState = indexState.getShard(0);
    indexState.verifyStarted();

    // App should re-use a previous timestampSec if user does a
    // follow-on action, so that things relying on timestampSec
    // (e.g. dynamic range facet counts, recency blended
    // sorting) don't change as the user drills down / next
    // pages / etc.
    // TODO: implement timeStamp
    final long timestampSec;
    //        if (searchRequest.getTimeStamp() != 0.0) {
    //            timestampSec = searchRequest.getTimeStamp();
    //        } else
    {
      timestampSec = System.currentTimeMillis() / 1000;
    }

    var diagnostics = SearchResponse.Diagnostics.newBuilder();

    final Map<String, FieldDef> indexFields = indexState.getAllFields();
    final Map<String, VirtualFieldDef> virtualFields = getVirtualFields(shardState, searchRequest);
    final Map<String, FieldDef> queryFields =
        virtualFields.isEmpty() ? indexFields : new HashMap<>(indexFields);

    final Set<String> fields = new HashSet<>();
    final Map<String, FieldHighlightConfig> highlightFields = new HashMap<>();
    boolean forceDocScores = false;

    for (Map.Entry<String, VirtualFieldDef> entry : virtualFields.entrySet()) {
      if (indexFields.containsKey(entry.getKey())) {
        throw new SearchHandlerException(
            String.format("Virtual field has a duplicate name: %s", entry.getKey()));
      }
      queryFields.put(entry.getKey(), entry.getValue());
      if (entry.getValue().getValuesSource().needsScores()) {
        forceDocScores = true;
      }
      fields.add(entry.getKey());
    }

    Query q = extractQuery(indexState, searchRequest, timestampSec);

    if (!searchRequest.getRetrieveFieldsList().isEmpty()) {
      Set<String> fieldSeen = new HashSet<>();
      for (Object o : searchRequest.getRetrieveFieldsList()) {
        String field;
        String highlight = "no";
        FieldHighlightConfig perField = null;
        if (o instanceof String) {
          field = (String) o;
          fields.add(field);
        } else {
          throw new UnsupportedOperationException(
              "retrieveFields, unrecognized object. Does not support highlighting fields yet at query time");
        }
        if (fieldSeen.contains(field)) {
          throw new SearchHandlerException(
              String.format("retrieveField has a duplicate field: %s", field));
        }
        fieldSeen.add(field);

        FieldDef fd = indexFields.get(field);
        if (fd == null) {
          throw new SearchHandlerException(
              String.format(
                  "retrieveFields, field: %s was not registered and was not specified as a dynamicField",
                  field));
        }

        // If any of the fields being retrieved require
        // score, than force returned FieldDoc.score to be
        // computed:
        if (fd instanceof VirtualFieldDef
            && ((VirtualFieldDef) fd).getValuesSource().needsScores()) {
          forceDocScores = true;
        }

        if (fd instanceof IndexableFieldDef) {
          IndexableFieldDef indexableField = (IndexableFieldDef) fd;

          if (perField != null) {
            perField.multiValued = indexableField.isMultiValue();
            if (indexableField.isMultiValue() == false && perField.mode.equals("joinedSnippets")) {
              throw new SearchHandlerException(
                  "highlight: joinedSnippets can only be used with multi-valued fields");
            }
          }
          if (!highlight.equals("no")
              && (indexableField instanceof TextBaseFieldDef)
              && !((TextBaseFieldDef) fd).isHighlighted()) {
            throw new SearchHandlerException(
                String.format(
                    "retrieveFields: field: %s was not indexed with highlight=true", field));
          }

          // nocommit allow pulling from DV?  need separate
          // dvFields?

          if (!indexableField.isStored() && !indexableField.hasDocValues()) {
            throw new SearchHandlerException(
                String.format(
                    "retrieveFields, field: %s was not registered with store=true or docValues=True",
                    field));
          }
        }
      }
    }

    // TODO: support this. Seems like lucene 8.2 has no PostingsHighlighter anymore?
    // HighlighterConfig highlighter = getHighlighter(indexState, r, highlightFields);

    diagnostics.setParsedQuery(q.toString());

    TopDocs hits;
    TopGroups<BytesRef> groups;
    TopGroups<Integer> joinGroups;
    int totalGroupCount = -1;

    String resultString;
    SearchResponse.Builder searchResponse = SearchResponse.newBuilder();

    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    // matching finally clause releases this searcher:
    try {
      // Pull the searcher we will use
      s = getSearcherAndTaxonomy(searchRequest, shardState, diagnostics, threadPoolExecutor);
      // nocommit can we ... not do this?  it's awkward that
      // we have to ... but, the 2-pass (query time
      // join/grouping) is slower for MTQs if we don't
      // ... and the whole out-of-order collector or not
      // ...

      q = s.searcher.rewrite(q);
      logger.debug(String.format("after rewrite, query: %s", q.toString()));
      diagnostics.setRewrittenQuery(q.toString());

      // nocommit add test with drill down on OR of fields:

      // TODO: re-enable this?  else we never get
      // in-order collectors
      // Weight w = s.createNormalizedWeight(q2);

      DrillDownQuery ddq = addDrillDowns(timestampSec, indexState, searchRequest, q);

      diagnostics.setDrillDownQuery(ddq.toString());

      Collector collector;
      // FIXME? not sure if these two groupCollectors are correct?
      FirstPassGroupingCollector groupCollector = null;
      AllGroupsCollector allGroupsCollector = null;

      FieldDef groupField = null;
      Sort groupSort = null;
      Sort sort;
      QuerySortField sortRequest;
      List<String> sortFieldNames;
      if (!searchRequest.getQuerySort().getFields().getSortedFieldsList().isEmpty()) {
        sortRequest = searchRequest.getQuerySort();
        sortFieldNames = new ArrayList<String>();
        sort =
            parseSort(
                timestampSec,
                indexState,
                searchRequest.getQuerySort().getFields().getSortedFieldsList(),
                sortFieldNames,
                queryFields);
      } else {
        sortRequest = null;
        sort = null;
        sortFieldNames = null;
      }

      int topHits = searchRequest.getTopHits();
      int totalHitsThreshold = TOTAL_HITS_THRESHOLD;
      if (searchRequest.getTotalHitsThreshold() != 0) {
        totalHitsThreshold = searchRequest.getTotalHitsThreshold();
      }

      CollectorManager<? extends Collector, ? extends TopDocs> collectorManager = null;

      // TODO: support "grouping" and "useBlockJoinCollector"
      if (sort == null) {
        // TODO: support "searchAfter" when supplied by user
        FieldDoc searchAfter = null;
        collector = TopScoreDocCollector.create(topHits, searchAfter, totalHitsThreshold);
        collectorManager =
            TopScoreDocCollector.createSharedManager(topHits, searchAfter, totalHitsThreshold);
      } else if (q instanceof MatchAllDocsQuery) {
        collector = new LargeNumHitsTopDocsCollector(topHits);
        collectorManager = LargeNumHitsTopDocsCollectorManagerCreator.createSharedManager(topHits);
      } else {

        // If any of the sort fields require score, than
        // ask for FieldDoc.score in the returned hits:
        for (SortField sortField : sort.getSort()) {
          forceDocScores |= sortField.needsScores();
        }

        // Sort by fields:
        // TODO: support "searchAfter" when supplied by user
        FieldDoc searchAfter;
        searchAfter = null;
        collector = TopFieldCollector.create(sort, topHits, searchAfter, totalHitsThreshold);
        collectorManager =
            TopFieldCollector.createSharedManager(sort, topHits, searchAfter, totalHitsThreshold);
      }

      long timeoutMS;
      /* TODO: fixme; we dont use timeOut as of now
          would need new CollectorManager impl that returns TimeLimitingCollector on newCollector() call
          e.g. new impls similar to TopFieldCollector.createSharedManager and TopScoreDocCollector.createSharedManager
      */
      Collector c2;
      if (searchRequest.getTimeoutSec() != 0.0) {
        timeoutMS = (long) (searchRequest.getTimeoutSec() * 1000);
        if (timeoutMS <= 0) {
          throw new SearchHandlerException("timeoutSec must be > 0 msec");
        }
        c2 =
            new TimeLimitingCollector(
                collector, TimeLimitingCollector.getGlobalCounter(), timeoutMS);
      } else {
        c2 = collector;
        timeoutMS = -1;
      }

      // nocommit can we do better?  sometimes downgrade
      // to DDQ not DS?

      long searchStartTime = System.nanoTime();

      // TODO: If "facets" create DrillSideways(ds) and do ds.search(ddq, c2)
      TopDocs topDocs = null;
      if (!searchRequest.getFacetsList().isEmpty()) {
        List<FacetResult> grpcFacetResults = new ArrayList<>();
        DrillSideways drillS =
            new DrillSidewaysImpl(
                s.searcher,
                indexState.facetsConfig,
                s.taxonomyReader,
                searchRequest.getFacetsList(),
                s,
                shardState,
                queryFields,
                grpcFacetResults,
                threadPoolExecutor);
        DrillSideways.ConcurrentDrillSidewaysResult<? extends TopDocs>
            concurrentDrillSidewaysResult = drillS.search(ddq, collectorManager);
        topDocs = concurrentDrillSidewaysResult.collectorResult;
        searchResponse.addAllFacetResult(grpcFacetResults);
      } else {
        try {
          topDocs = s.searcher.search(ddq, collectorManager);
        } catch (TimeLimitingCollector.TimeExceededException tee) {
          searchResponse.setHitTimeout(true);
        }
      }

      diagnostics.setFirstPassSearchTimeMs(((System.nanoTime() - searchStartTime) / 1000000.0));

      int startHit = searchRequest.getStartHit();

      // TODO: support "grouping" and "useBlockJoinCollector" (we need a new collector for grouping
      // and blockJoin)
      // else do this...
      {
        groups = null;
        joinGroups = null;
        hits = topDocs;

        if (startHit != 0) {
          // Slice:
          int count = Math.max(0, hits.scoreDocs.length - startHit);
          ScoreDoc[] newScoreDocs = new ScoreDoc[count];
          if (count > 0) {
            System.arraycopy(hits.scoreDocs, startHit, newScoreDocs, 0, count);
          }
          hits = new TopDocs(hits.totalHits, newScoreDocs);
        }
      }

      int[] highlightDocIDs = null;
      // TODO: if "groupField!=null" collect group counts as well
      {
        highlightDocIDs = new int[hits.scoreDocs.length];
        for (int i = 0; i < hits.scoreDocs.length; i++) {
          highlightDocIDs[i] = hits.scoreDocs[i].doc;
        }
      }

      Map<String, Object[]> highlights = null;

      long t0 = System.nanoTime();
      if (highlightDocIDs != null && highlightFields != null && !highlightFields.isEmpty()) {
        // TODO
        // highlights = highlighter to objects
      }
      diagnostics.setHighlightTimeMs((System.nanoTime() - t0) / 1000000.);

      t0 = System.nanoTime();

      // TODO: deal with fillFields for group!=null and useBlockJoin
      {
        TotalHits totalHits =
            TotalHits.newBuilder()
                .setRelation(TotalHits.Relation.valueOf(hits.totalHits.relation.name()))
                .setValue(hits.totalHits.value)
                .build();
        searchResponse.setTotalHits(totalHits);
        for (int hitIndex = 0; hitIndex < hits.scoreDocs.length; hitIndex++) {
          ScoreDoc hit = hits.scoreDocs[hitIndex];
          var hitResponse = SearchResponse.Hit.newBuilder();
          hitResponse.setLuceneDocId(hit.doc);
          if (!Float.isNaN(hit.score)) {
            hitResponse.setScore(hit.score);
          }

          if (fields != null || highlightFields != null) {
            var fieldValueMap =
                fillFields(
                    indexState, null, s.searcher, hit, fields, highlights, hitIndex, queryFields);
            var sortedFields = getSortedFieldsForHit(hit, sort, sortFieldNames);
            hitResponse.putAllFields(fieldValueMap);
            hitResponse.putAllSortedFields(sortedFields);
          }
          searchResponse.addHits(hitResponse);
        }
      }

      SearchState.Builder searchState = SearchState.newBuilder();
      searchState.setTimestamp(timestampSec);

      // Record searcher version that handled this request:
      searchState.setSearcherVersion(((DirectoryReader) s.searcher.getIndexReader()).getVersion());

      // Fill in lastDoc for searchAfter:
      if (hits != null && hits.scoreDocs.length != 0) {
        ScoreDoc lastHit = hits.scoreDocs[hits.scoreDocs.length - 1];
        searchState.setLastDocId(lastHit.doc);
        if (sort != null) {
          FieldDoc fd = (FieldDoc) lastHit;
          for (Object fv : fd.fields) {
            searchState.addLastFieldValues(fv.toString());
          }
        } else {
          searchState.setLastScore(lastHit.score);
        }
      }

      diagnostics.setGetFieldsTimeMs(((System.nanoTime() - t0) / 1000000));

      searchResponse.setDiagnostics(diagnostics);
      searchResponse.setSearchState(searchState);
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

    return searchResponse.build();
  }

  /** Parses any virtualFields, which define dynamic (expression) fields for this one request. */
  private static Map<String, VirtualFieldDef> getVirtualFields(
      ShardState shardState, SearchRequest searchRequest) {
    if (searchRequest.getVirtualFieldsList().isEmpty()) {
      return Collections.emptyMap();
    }
    IndexState indexState = shardState.indexState;
    Map<String, VirtualFieldDef> virtualFields = new HashMap<>();
    for (VirtualField vf : searchRequest.getVirtualFieldsList()) {
      if (virtualFields.containsKey(vf.getName())) {
        throw new IllegalArgumentException(
            "Multiple definitions of Virtual field: " + vf.getName());
      }
      ScoreScript.Factory factory =
          ScriptService.getInstance().compile(vf.getScript(), ScoreScript.CONTEXT);
      Map<String, Object> params =
          Maps.transformValues(vf.getScript().getParamsMap(), ScriptParamsTransformer.INSTANCE);
      VirtualFieldDef virtualField =
          new VirtualFieldDef(vf.getName(), factory.newFactory(params, indexState.docLookup));
      virtualFields.put(vf.getName(), virtualField);
    }
    return virtualFields;
  }

  private static Query extractQuery(
      IndexState state, SearchRequest searchRequest, long timestampSec)
      throws SearchHandlerException {
    Query q;
    if (!searchRequest.getQueryText().isEmpty()) {
      QueryBuilder queryParser = createQueryParser(state, searchRequest, null);

      String queryText = searchRequest.getQueryText();

      try {
        q = parseQuery(queryParser, queryText);
      } catch (Exception e) {
        throw new SearchHandlerException(String.format("could not parse queryText: %s", queryText));
      }
    } else if (searchRequest.getQuery().getQueryNodeCase()
        != com.yelp.nrtsearch.server.grpc.Query.QueryNodeCase.QUERYNODE_NOT_SET) {
      q = QUERY_NODE_MAPPER.getQuery(searchRequest.getQuery(), state);
    } else {
      q = new MatchAllDocsQuery();
    }

    return q;
  }

  /** If field is non-null it overrides any specified defaultField. */
  private static QueryBuilder createQueryParser(
      IndexState state, SearchRequest searchRequest, String field) {
    // TODO: Support "queryParser" field provided by user e.g. MultiFieldQueryParser,
    // SimpleQueryParser, classic
    List<String> fields;
    if (field != null) {
      fields = Collections.singletonList(field);
    } else {
      // Default to MultiFieldQueryParser over all indexed fields:
      fields = state.getIndexedAnalyzedFields();
    }
    return new MultiFieldQueryParser(
        fields.toArray(new String[fields.size()]), state.searchAnalyzer);
  }

  private static Query parseQuery(QueryBuilder qp, String text)
      throws ParseException, org.apache.lucene.queryparser.classic.ParseException {
    if (qp instanceof QueryParserBase) {
      return ((QueryParserBase) qp).parse(text);
    } else {
      return ((SimpleQueryParser) qp).parse(text);
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
        diagnostics.setNrtWaitTimeMs((System.nanoTime() - t0) / 1000000);
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

  /** Fold in any drillDowns requests into the query. */
  private static DrillDownQuery addDrillDowns(
      long timestampSec, IndexState state, SearchRequest searchRequest, Query q) {
    // TOOD: support "drillDowns" in input SearchRequest
    // Always create a DrillDownQuery; if there
    // are no drill-downs it will just rewrite to the
    // original query:
    DrillDownQuery ddq = new DrillDownQuery(state.facetsConfig, q);
    return ddq;
  }

  /** Decodes a list of Request into the corresponding Sort. */
  static Sort parseSort(
      long timestampSec,
      IndexState state,
      List<SortType> fields,
      List<String> sortFieldNames,
      Map<String, FieldDef> queryFields)
      throws SearchHandlerException {
    List<SortField> sortFields = new ArrayList<SortField>();
    for (SortType sub : fields) {
      String fieldName = sub.getFieldName();
      SortField sf;
      if (sortFieldNames != null) {
        sortFieldNames.add(fieldName);
      }
      if (fieldName.equals("docid")) {
        sf = SortField.FIELD_DOC;
      } else if (fieldName.equals("score")) {
        sf = SortField.FIELD_SCORE;
      } else {
        FieldDef fd = queryFields.get(fieldName);
        if (fd == null) {
          throw new SearchHandlerException(
              String.format(
                  "field: %s was not registered and was not specified as a virtualField",
                  fieldName));
        }

        if (!(fd instanceof Sortable)) {
          throw new SearchHandlerException(
              String.format("field: %s does not support sorting", fieldName));
        }

        sf = ((Sortable) fd).getSortField(sub);
      }
      sortFields.add(sf);
    }
    return new Sort(sortFields.toArray(new SortField[0]));
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
      ScoreDoc hit,
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
          List<LeafReaderContext> leaves = s.getIndexReader().leaves();
          LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(hit.doc, leaves));

          int docID = hit.doc - leaf.docBase;

          assert !Float.isNaN(hit.score) || !virtualFieldDef.getValuesSource().needsScores();
          DoubleValues scoreValue =
              new DoubleValues() {
                @Override
                public double doubleValue() throws IOException {
                  return hit.score;
                }

                @Override
                public boolean advanceExact(int doc) throws IOException {
                  return !Float.isNaN(hit.score);
                }
              };
          DoubleValues doubleValues = virtualFieldDef.getValuesSource().getValues(leaf, scoreValue);
          doubleValues.advanceExact(docID);
          compositeFieldValue.addFieldValue(
              FieldValue.newBuilder().setDoubleValue(doubleValues.doubleValue()));
        } else if (fd instanceof IndexableFieldDef && ((IndexableFieldDef) fd).hasDocValues()) {
          List<LeafReaderContext> leaves = s.getIndexReader().leaves();
          // get the current leaf/segment that this doc is in
          LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(hit.doc, leaves));
          int docID = hit.doc - leaf.docBase;
          // it may be possible to cache this if there are multiple hits in the same segment
          LoadedDocValues<?> docValues = ((IndexableFieldDef) fd).getDocValues(leaf);
          docValues.setDocId(docID);
          for (int i = 0; i < docValues.size(); ++i) {
            compositeFieldValue.addFieldValue(docValues.toFieldValue(i));
          }
        }
        // retrieve stored fields
        else if (fd instanceof IndexableFieldDef && ((IndexableFieldDef) fd).isStored()) {
          String[] values = ((IndexableFieldDef) fd).getStored(s.doc(hit.doc));
          for (String fieldValue : values) {
            compositeFieldValue.addFieldValue(FieldValue.newBuilder().setTextValue(fieldValue));
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

  private Map<String, CompositeFieldValue> getSortedFieldsForHit(
      ScoreDoc hit, Sort sort, List<String> sortFieldNames) {
    var sortedFields = new HashMap<String, CompositeFieldValue>();
    if (hit instanceof FieldDoc) {
      FieldDoc fd = (FieldDoc) hit;
      if (fd.fields != null) {
        SortField[] sortFields = sort.getSort();

        for (int i = 0; i < sortFields.length; i++) {
          // We must use a separate list because an expr's
          // SortField doesn't know the virtual field name
          // (it returns the expression string from
          // .getField):
          String fieldName = sortFieldNames.get(i);
          String value;

          if (fd.fields[i] instanceof BytesRef) {
            value = ((BytesRef) fd.fields[i]).utf8ToString();
          } else {
            // FIXME: not sure this is serializable to string?
            value = fd.fields[i].toString();
          }
          var compositeFieldValue =
              CompositeFieldValue.newBuilder()
                  .addFieldValue(FieldValue.newBuilder().setTextValue(value))
                  .build();
          sortedFields.put(fieldName, compositeFieldValue);
        }
      }
    }

    return sortedFields;
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

  /** Highlight configuration. */
  static class FieldHighlightConfig {
    /** Number of passages. */
    public int maxPassages = -1;

    // nocommit use enum:
    /** Snippet or whole. */
    public String mode;

    /** True if field is single valued. */
    public boolean multiValued;

    /** {@link BreakIterator} to use. */
    public BreakIterator breakIterator;
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
