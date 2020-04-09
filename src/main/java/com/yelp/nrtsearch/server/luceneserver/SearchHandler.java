/*
 *
 *  * Copyright 2019 Yelp Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  * either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.Point;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.QueryType;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.SearchState;
import com.yelp.nrtsearch.server.grpc.Selector;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.grpc.TotalHits;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.grouping.AllGroupsCollector;
import org.apache.lucene.search.grouping.FirstPassGroupingCollector;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

public class SearchHandler implements Handler<SearchRequest, SearchResponse> {
    Logger logger = LoggerFactory.getLogger(RegisterFieldsHandler.class);
    /**
     * By default we count hits accurately up to 1000. This makes sure that we
     * don't spend most time on computing hit counts
     */
    private static final int TOTAL_HITS_THRESHOLD = 1000;

    private static final QueryNodeMapper QUERY_NODE_MAPPER = new QueryNodeMapper();

    @Override
    public SearchResponse handle(IndexState indexState, SearchRequest searchRequest) throws SearchHandlerException {
        ShardState shardState = indexState.getShard(0);
        indexState.verifyStarted();

        // App should re-use a previous timestampSec if user does a
        // follow-on action, so that things relying on timestampSec
        // (e.g. dynamic range facet counts, recency blended
        // sorting) don't change as the user drills down / next
        // pages / etc.
        //TODO: implement timeStamp
        final long timestampSec;
//        if (searchRequest.getTimeStamp() != 0.0) {
//            timestampSec = searchRequest.getTimeStamp();
//        } else
        {
            timestampSec = System.currentTimeMillis() / 1000;
        }

        var diagnostics = SearchResponse.Diagnostics.newBuilder();

        final Map<String, FieldDef> dynamicFields = getDynamicFields(shardState, searchRequest);

        Query q = extractQuery(indexState, searchRequest, timestampSec, dynamicFields);


        final Set<String> fields;
        final Map<String, FieldHighlightConfig> highlightFields;
        boolean forceDocScores = false;

        if (!searchRequest.getRetrieveFieldsList().isEmpty()) {
            fields = new HashSet<String>();
            highlightFields = new HashMap<String, FieldHighlightConfig>();
            Set<String> fieldSeen = new HashSet<String>();
            for (Object o : searchRequest.getRetrieveFieldsList()) {
                String field;
                String highlight = "no";
                FieldHighlightConfig perField = null;
                if (o instanceof String) {
                    field = (String) o;
                    fields.add(field);
                } else {
                    throw new UnsupportedOperationException("retrieveFields, unrecognized object. Does not support highlighting fields yet at query time");
                }
                if (fieldSeen.contains(field)) {
                    throw new SearchHandlerException(String.format("retrieveField has a duplicate field: %s", field));
                }
                fieldSeen.add(field);

                FieldDef fd = dynamicFields.get(field);
                if (fd == null) {
                    throw new SearchHandlerException(String.format("retrieveFields, field: %s was not registered and was not specified as a dynamicField", field));
                }

                // If any of the fields being retrieved require
                // score, than force returned FieldDoc.score to be
                // computed:
                if (fd.valueSource != null && fd.valueSource.getSortField(false).needsScores()) {
                    forceDocScores = true;
                }

                if (perField != null) {
                    perField.multiValued = fd.multiValued;
                    if (fd.multiValued == false && perField.mode.equals("joinedSnippets")) {
                        throw new SearchHandlerException("highlight: joinedSnippets can only be used with multi-valued fields");
                    }
                }
                if (!highlight.equals("no") && !fd.highlighted) {
                    throw new SearchHandlerException(String.format("retrieveFields: field: %s was not indexed with highlight=true", field));
                }

                // nocommit allow pulling from DV?  need separate
                // dvFields?

                if (fd.fieldType == null) {
                    if (fd.valueSource == null) {
                        throw new SearchHandlerException(String.format("retrieveFields, field: %s was not registered with store=true", field));
                    }
                } else if (!fd.fieldType.stored() && DocValuesType.NONE == fd.fieldType.docValuesType()) {
                    throw new SearchHandlerException(String.format("retrieveFields, field: %s was not registered with store=true or docValues=True", field));
                }
            }

        } else {
            fields = null;
            highlightFields = null;
        }

        //TODO: support this. Seems like lucene 8.2 has no PostingsHighlighter anymore?
        //HighlighterConfig highlighter = getHighlighter(indexState, r, highlightFields);

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
            s = getSearcherAndTaxonomy(searchRequest, shardState, diagnostics);
            // nocommit can we ... not do this?  it's awkward that
            // we have to ... but, the 2-pass (query time
            // join/grouping) is slower for MTQs if we don't
            // ... and the whole out-of-order collector or not
            // ...

            q = s.searcher.rewrite(q);
            logger.info(String.format("after rewrite, query: %s", q.toString()));
            diagnostics.setRewrittenQuery(q.toString());

            // nocommit add test with drill down on OR of fields:

            // TODO: re-enable this?  else we never get
            // in-order collectors
            //Weight w = s.createNormalizedWeight(q2);

            DrillDownQuery ddq = addDrillDowns(timestampSec, indexState, searchRequest, q, dynamicFields);

            diagnostics.setDrillDownQuery(ddq.toString());

            Collector collector;
            //FIXME? not sure if these two groupCollectors are correct?
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
                sort = parseSort(timestampSec, indexState, searchRequest.getQuerySort().getFields().getSortedFieldsList(), sortFieldNames, dynamicFields);
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

            CollectorManager<TopScoreDocCollector, TopDocs> topDocsCollectorManager = null;
            CollectorManager<TopFieldCollector, TopFieldDocs> topFieldDocsCollectorManager = null;

            //TODO: support "grouping" and "useBlockJoinCollector"
            if (sort == null) {
                //TODO: support "searchAfter" when supplied by user
                FieldDoc searchAfter = null;
                collector = TopScoreDocCollector.create(topHits, searchAfter, totalHitsThreshold);
                topDocsCollectorManager = TopScoreDocCollector.createSharedManager(topHits, searchAfter, totalHitsThreshold);
            } else {

                // If any of the sort fields require score, than
                // ask for FieldDoc.score in the returned hits:
                for (SortField sortField : sort.getSort()) {
                    forceDocScores |= sortField.needsScores();
                }

                // Sort by fields:
                //TODO: support "searchAfter" when supplied by user
                FieldDoc searchAfter;
                searchAfter = null;
                collector = TopFieldCollector.create(sort, topHits, searchAfter, totalHitsThreshold);
                topFieldDocsCollectorManager = TopFieldCollector.createSharedManager(sort, topHits, searchAfter, totalHitsThreshold);
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
                c2 = new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), timeoutMS);
            } else {
                c2 = collector;
                timeoutMS = -1;
            }

            // nocommit can we do better?  sometimes downgrade
            // to DDQ not DS?

            long searchStartTime = System.nanoTime();

            //TODO: If "facets" create DrillSideways(ds) and do ds.search(ddq, c2)
            //else if not facets...
            TopDocs topDocs= null;
            {
                try {
                    if (topDocsCollectorManager != null) {
                        topDocs = s.searcher.search(ddq, topDocsCollectorManager);
                    } else {  //topFieldDocsCollectorManager != null
                        topDocs = s.searcher.search(ddq, topFieldDocsCollectorManager);
                    }
                } catch (TimeLimitingCollector.TimeExceededException tee) {
                    searchResponse.setHitTimeout(true);
                }
            }

            diagnostics.setFirstPassSearchTimeMs(((System.nanoTime() - searchStartTime) / 1000000.0));

            int startHit = searchRequest.getStartHit();

            //TODO: support "grouping" and "useBlockJoinCollector" (we need a new collector for grouping and blockJoin)
            //else do this...
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
            //TODO: if "groupField!=null" collect group counts as well
            {
                highlightDocIDs = new int[hits.scoreDocs.length];
                for (int i = 0; i < hits.scoreDocs.length; i++) {
                    highlightDocIDs[i] = hits.scoreDocs[i].doc;
                }
            }

            Map<String, Object[]> highlights = null;

            long t0 = System.nanoTime();
            if (highlightDocIDs != null && highlightFields != null && !highlightFields.isEmpty()) {
                //TODO
                //highlights = highlighter to objects
            }
            diagnostics.setHighlightTimeMs((System.nanoTime() - t0) / 1000000.);

            t0 = System.nanoTime();

            //TODO: deal with fillFields for group!=null and useBlockJoin
            {
                TotalHits totalHits = TotalHits.newBuilder()
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
                        var fieldValueMap = fillFields(indexState, null, s.searcher, hit, fields, highlights, hitIndex, dynamicFields);
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


    /**
     * Parses any virtualFields, which define dynamic
     * (expression) fields for this one request.
     */
    private static Map<String, FieldDef> getDynamicFields(ShardState shardState, SearchRequest searchRequest) {
        IndexState indexState = shardState.indexState;
        Map<String, FieldDef> dynamicFields = null;
        if (!searchRequest.getVirtualFielsdList().isEmpty()) {
            throw new UnsupportedOperationException(String.format("VirtualFields not currently supported in searchRequest: %s", searchRequest.toString()));
        } else {
            dynamicFields = indexState.getAllFields();
        }
        return dynamicFields;
    }

    private static Query extractQuery(IndexState state, SearchRequest searchRequest, long timestampSec, Map<String, FieldDef> dynamicFields) throws SearchHandlerException {
        Query q;
        if (!searchRequest.getQueryText().isEmpty()) {
            QueryBuilder queryParser = createQueryParser(state, searchRequest, null);

            String queryText = searchRequest.getQueryText();

            try {
                q = parseQuery(queryParser, queryText);
            } catch (Exception e) {
                throw new SearchHandlerException(String.format("could not parse queryText: %s", queryText));
            }
        } else if (searchRequest.getQuery().getQueryType() != QueryType.NONE) {
            q = QUERY_NODE_MAPPER.getQuery(searchRequest.getQuery(), state);
        } else {
            q = new MatchAllDocsQuery();
        }

        return q;
    }

    /**
     * If field is non-null it overrides any specified
     * defaultField.
     */
    private static QueryBuilder createQueryParser(IndexState state, SearchRequest searchRequest, String field) {
        //TODO: Support "queryParser" field provided by user e.g. MultiFieldQueryParser, SimpleQueryParser, classic
        List<String> fields;
        if (field != null) {
            fields = Collections.singletonList(field);
        } else {
            // Default to MultiFieldQueryParser over all indexed fields:
            fields = state.getIndexedAnalyzedFields();
        }
        return new MultiFieldQueryParser(fields.toArray(new String[fields.size()]), state.searchAnalyzer);
    }

    private static Query parseQuery(QueryBuilder qp, String text) throws ParseException, org.apache.lucene.queryparser.classic.ParseException {
        if (qp instanceof QueryParserBase) {
            return ((QueryParserBase) qp).parse(text);
        } else {
            return ((SimpleQueryParser) qp).parse(text);
        }
    }

    /**
     * Returns the requested searcher + taxoReader, either
     * by indexGen, snapshot, version or just the current
     * (latest) one.
     */
    public static SearcherTaxonomyManager.SearcherAndTaxonomy getSearcherAndTaxonomy(SearchRequest searchRequest, ShardState state, SearchResponse.Diagnostics.Builder diagnostics) throws InterruptedException, IOException {
        Logger logger = LoggerFactory.getLogger(SearcherTaxonomyManager.SearcherAndTaxonomy.class);
        //TODO: Figure out which searcher to use:
        //final long searcherVersion; e.g. searcher.getLong("version")
        //final IndexState.Gens searcherSnapshot; e.g. searcher.getLong("indexGen")
        //Currently we only use the current(latest) searcher
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
                    s = openSnapshotReader(state, snapshot, diagnostics);
                } else {
                    SearcherTaxonomyManager.SearcherAndTaxonomy current = state.acquire();
                    long currentVersion = ((DirectoryReader) current.searcher.getIndexReader()).getVersion();
                    if (currentVersion == version) {
                        s = current;
                    } else if (version > currentVersion) {
                        logger.info("SearchHandler: now await version=" + version + " vs currentVersion=" + currentVersion);

                        // TODO: should we have some timeout here? if user passes bogus future version, we hang forever:

                        // user is asking for search version beyond what we are currently searching ... wait for us to refresh to it:

                        state.release(current);

                        // TODO: Use FutureTask<SearcherAndTaxonomy> here?

                        // nocommit: do this in an async way instead!  this task should be parked somewhere and resumed once refresh runs and exposes
                        // the requested version, instead of blocking the current search thread
                        Lock lock = new ReentrantLock();
                        Condition cond = lock.newCondition();
                        ReferenceManager.RefreshListener listener = new ReferenceManager.RefreshListener() {
                            @Override
                            public void beforeRefresh() {
                            }

                            @Override
                            public void afterRefresh(boolean didRefresh) throws IOException {
                                SearcherTaxonomyManager.SearcherAndTaxonomy current = state.acquire();
                                logger.info("SearchHandler: refresh completed newVersion=" + ((DirectoryReader) current.searcher.getIndexReader()).getVersion());
                                try {
                                    if (((DirectoryReader) current.searcher.getIndexReader()).getVersion() >= version) {
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
                                logger.info("SearchHandler: await released,  current version " + ((DirectoryReader) current.searcher.getIndexReader()).getVersion() + " required minimum version " + version);
                                assert ((DirectoryReader) current.searcher.getIndexReader()).getVersion() >= version;
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
                        throw new RuntimeException("searcher: This searcher has expired version=" + version + " vs currentVersion=" + currentVersion);
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
                throw new RuntimeException("indexGen: requested indexGen (" + gen + ") is beyond the current maximum generation (" + state.writer.getMaxCompletedSequenceNumber() + ")");
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

    /**
     * Returns a ref.
     */
    private static SearcherTaxonomyManager.SearcherAndTaxonomy openSnapshotReader(ShardState state, IndexState.Gens snapshot, SearchResponse.Diagnostics.Builder diagnostics) throws IOException {
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
            IndexReader r = DirectoryReader.openIfChanged((DirectoryReader) s.searcher.getIndexReader(),
                    state.snapshots.getIndexCommit(snapshot.indexGen));

            // Ref that we return to caller
            s.taxonomyReader.incRef();

            SearcherTaxonomyManager.SearcherAndTaxonomy result = new SearcherTaxonomyManager.SearcherAndTaxonomy(new MyIndexSearcher(r), s.taxonomyReader);
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
     * Fold in any drillDowns requests into the query.
     */
    private static DrillDownQuery addDrillDowns(long timestampSec, IndexState state, SearchRequest searchRequest, Query q, Map<String, FieldDef> dynamicFields) {
        //TOOD: support "drillDowns" in input SearchRequest
        // Always create a DrillDownQuery; if there
        // are no drill-downs it will just rewrite to the
        // original query:
        DrillDownQuery ddq = new DrillDownQuery(state.facetsConfig, q);
        return ddq;
    }

    /**
     * Decodes a list of Request into the corresponding Sort.
     */
    static Sort parseSort(long timestampSec, IndexState state, List<SortType> fields, List<String> sortFieldNames, Map<String, FieldDef> dynamicFields) throws SearchHandlerException {
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
                FieldDef fd;
                if (dynamicFields != null) {
                    fd = dynamicFields.get(fieldName);
                } else {
                    fd = null;
                }
                if (fd == null) {
                    fd = state.getField(fieldName);
                }
                if (fd == null) {
                    throw new SearchHandlerException(String.format("field: %s was not registered and was not specified as a dynamicField", fieldName));
                }

                if (fd.valueSource != null) {
                    sf = fd.valueSource.getSortField(sub.getReverse());
                } else if (fd.valueType == FieldDef.FieldValueType.LAT_LON) {
                    if (fd.fieldType.docValuesType() == DocValuesType.NONE) {
                        throw new SearchHandlerException(String.format("field: %s was not registered with sort=true", fieldName));
                    }
                    Point sub2 = sub.getOrigin();
                    sf = LatLonDocValuesField.newDistanceSort(fieldName, sub2.getLatitude(), sub2.getLongitude());
                } else {
                    if ((fd.fieldType != null && fd.fieldType.docValuesType() == DocValuesType.NONE) ||
                            (fd.fieldType == null && fd.valueSource == null)) {
                        throw new SearchHandlerException(String.format("field: %s was not registered with sort=true", fieldName));
                    }

                    if (fd.multiValued) {
                        Selector selectorString = sub.getSelector();
                        if (fd.valueType == FieldDef.FieldValueType.ATOM) {
                            SortedSetSelector.Type selector;
                            if (selectorString.equals(Selector.MIN)) {
                                selector = SortedSetSelector.Type.MIN;
                            } else if (selectorString.equals(Selector.MAX)) {
                                selector = SortedSetSelector.Type.MAX;
                            } else if (selectorString.equals(Selector.MIDDLE_MIN)) {
                                selector = SortedSetSelector.Type.MIDDLE_MIN;
                            } else if (selectorString.equals(Selector.MIDDLE_MAX)) {
                                selector = SortedSetSelector.Type.MIDDLE_MAX;
                            } else {
                                assert false;
                                // dead code but javac disagrees
                                selector = null;
                            }
                            sf = new SortedSetSortField(fieldName, sub.getReverse(), selector);
                        } else if (fd.valueType == FieldDef.FieldValueType.INT) {
                            sf = new SortedNumericSortField(fieldName, SortField.Type.INT, sub.getReverse(), parseNumericSelector(selectorString));
                        } else if (fd.valueType == FieldDef.FieldValueType.LONG) {
                            sf = new SortedNumericSortField(fieldName, SortField.Type.LONG, sub.getReverse(), parseNumericSelector(selectorString));
                        } else if (fd.valueType == FieldDef.FieldValueType.FLOAT) {
                            sf = new SortedNumericSortField(fieldName, SortField.Type.FLOAT, sub.getReverse(), parseNumericSelector(selectorString));
                        } else if (fd.valueType == FieldDef.FieldValueType.DOUBLE) {
                            sf = new SortedNumericSortField(fieldName, SortField.Type.DOUBLE, sub.getReverse(), parseNumericSelector(selectorString));
                        } else {
                            throw new SearchHandlerException(String.format("cannot sort by multiValued field: %s tyep is %s", fieldName, fd.valueType));
                        }
                    } else {
                        SortField.Type sortType;
                        if (fd.valueType == FieldDef.FieldValueType.ATOM) {
                            sortType = SortField.Type.STRING;
                        } else if (fd.valueType == FieldDef.FieldValueType.LONG || fd.valueType == FieldDef.FieldValueType.DATE_TIME) {
                            sortType = SortField.Type.LONG;
                        } else if (fd.valueType == FieldDef.FieldValueType.INT) {
                            sortType = SortField.Type.INT;
                        } else if (fd.valueType == FieldDef.FieldValueType.DOUBLE) {
                            sortType = SortField.Type.DOUBLE;
                        } else if (fd.valueType == FieldDef.FieldValueType.FLOAT) {
                            sortType = SortField.Type.FLOAT;
                        } else {
                            throw new SearchHandlerException(String.format("cannot sort by field: %s tyep is %s", fieldName, fd.valueType));
                        }

                        sf = new SortField(fieldName,
                                sortType,
                                sub.getReverse());
                    }
                }

                boolean hasMissingLast = sub.getMissingLat();

                //TODO: SortType to have field missingLast?
                boolean missingLast = false;

                if (fd.valueType == FieldDef.FieldValueType.ATOM) {
                    if (missingLast) {
                        sf.setMissingValue(SortField.STRING_LAST);
                    } else {
                        sf.setMissingValue(SortField.STRING_FIRST);
                    }
                } else if (fd.valueType == FieldDef.FieldValueType.INT) {
                    sf.setMissingValue(missingLast ? Integer.MAX_VALUE : Integer.MIN_VALUE);
                } else if (fd.valueType == FieldDef.FieldValueType.LONG) {
                    sf.setMissingValue(missingLast ? Long.MAX_VALUE : Long.MIN_VALUE);
                } else if (fd.valueType == FieldDef.FieldValueType.FLOAT) {
                    sf.setMissingValue(missingLast ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY);
                } else if (fd.valueType == FieldDef.FieldValueType.DOUBLE) {
                    sf.setMissingValue(missingLast ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY);
                } else if (hasMissingLast) {
                    throw new SearchHandlerException(String.format("field: %s can only specify missingLast for string and numeric field types: got SortField type: %s ", fieldName, sf.getType()));
                }
            }
            sortFields.add(sf);
        }

        return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
    }

    private static SortedNumericSelector.Type parseNumericSelector(Selector selectorString) throws SearchHandlerException {
        if (selectorString.equals(Selector.MIN)) {
            return SortedNumericSelector.Type.MIN;
        } else if (selectorString.equals(Selector.MAX)) {
            return SortedNumericSelector.Type.MAX;
        } else {
            throw new SearchHandlerException("selector, must be min or max for multi-valued numeric sort fields");
        }
    }

    /**
     * Fills in the returned fields (some hilited) for one hit:
     *
     * @return
     */
    private Map<String, CompositeFieldValue> fillFields(IndexState state, Object highlighter, IndexSearcher s,
                                                        ScoreDoc hit, Set<String> fields,
                                                        Map<String, Object[]> highlights,
                                                        int hiliteHitIndex,
                                                        Map<String, FieldDef> dynamicFields) throws IOException {
        Map<String, CompositeFieldValue> fieldValueMap = new HashMap<>();
        if (fields != null) {
            // Add requested stored fields (no highlighting):

            // even if they were not stored ...
            //TODO: get highlighted fields as well
            // Map<String,Object> doc = highlighter.getDocument(state, s, hit.doc);
            Map<String, Object> doc = new HashMap<>();
            boolean docIdAdvanced = false;
            for (String name : fields) {
                CompositeFieldValue.Builder compositeFieldValue = CompositeFieldValue.newBuilder();
                FieldDef fd = dynamicFields.get(name);

                // We detect invalid field above:
                assert fd != null;

                // retrieve from doc values
                if (fd.valueSource != null) {
                    List<LeafReaderContext> leaves = s.getIndexReader().leaves();
                    LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(hit.doc, leaves));
                    Map<String, Object> context = new HashMap<String, Object>();

                    int docID = hit.doc - leaf.docBase;

                    assert Float.isNaN(hit.score) == false || fd.valueSource.getSortField(false).needsScores() == false;
                    context.put("scorer", new Scorer(null) {
                        @Override
                        public DocIdSetIterator iterator() {
                            return null;
                        }

                        @Override
                        public float getMaxScore(int upTo) throws IOException {
                            return hit.score;
                        }

                        @Override
                        public float score() throws IOException {
                            return hit.score;
                        }

                        @Override
                        public int docID() {
                            return docID;
                        }
                    });
                    DoubleValues doubleValues = fd.valueSource.getValues(leaf, null);
                    compositeFieldValue.addFieldValue(FieldValue.newBuilder()
                            .setDoubleValue(doubleValues.doubleValue()));
                } else if (fd.fieldType != null && fd.fieldType.docValuesType() != DocValuesType.NONE) {
                    List<LeafReaderContext> leaves = s.getIndexReader().leaves();
                    //get the current leaf/segment that this doc is in
                    LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(hit.doc, leaves));
                    int docID = -1;
                    boolean advance = false;
                    switch (fd.fieldType.docValuesType()) {
                        case SORTED_NUMERIC:
                            SortedNumericDocValues sortedNumericDocValues = DocValues.getSortedNumeric(leaf.reader(), name);
                            //get segment local docID since that is what advanceExact wants
                            docID = hit.doc - leaf.docBase;
                            advance = sortedNumericDocValues.advanceExact(docID);
                            if (advance) {
                                for (int i = 0; i < sortedNumericDocValues.docValueCount(); i++) {
                                    long val = sortedNumericDocValues.nextValue();
                                    setCompositeFieldValue(compositeFieldValue, fd.valueType, val, true);
                                }
                            }
                            break;
                        case NUMERIC:
                            NumericDocValues numericDocValues = DocValues.getNumeric(leaf.reader(), name);
                            //get segment local docID since that is what advanceExact wants
                            docID = hit.doc - leaf.docBase;
                            advance = numericDocValues.advanceExact(docID);
                            if (advance) {
                                long val = numericDocValues.longValue();
                                setCompositeFieldValue(compositeFieldValue, fd.valueType, val, false);
                            }
                            break;
                        case SORTED_SET:
                            SortedSetDocValues sortedSetDocValues = DocValues.getSortedSet(leaf.reader(), name);
                            //get segment local docID since that is what advanceExact wants
                            docID = hit.doc - leaf.docBase;
                            advance = sortedSetDocValues.advanceExact(docID);
                            if (advance) {
                                for (; ; ) {
                                    long ord = sortedSetDocValues.nextOrd();
                                    if (ord == NO_MORE_ORDS) {
                                        break;
                                    }
                                    BytesRef bytesRef = sortedSetDocValues.lookupOrd(ord);
                                    compositeFieldValue.addFieldValue(FieldValue.newBuilder()
                                            .setTextValue(bytesRef.utf8ToString()));
                                }
                            }
                            break;
                        case SORTED:
                            SortedDocValues sortedDocValues = DocValues.getSorted(leaf.reader(), name);
                            //get segment local docID since that is what advanceExact wants
                            docID = hit.doc - leaf.docBase;
                            advance = sortedDocValues.advanceExact(docID);
                            if (advance) {
                                int ord = sortedDocValues.ordValue();
                                BytesRef bytesRef = sortedDocValues.lookupOrd(ord);
                                compositeFieldValue.addFieldValue(FieldValue.newBuilder().setTextValue(bytesRef.utf8ToString()));
                            }
                            break;
                        case BINARY:
                            BinaryDocValues binaryDocValues = DocValues.getBinary(leaf.reader(), name);
                            //get segment local docID since that is what advanceExact wants
                            docID = hit.doc - leaf.docBase;
                            advance = binaryDocValues.advanceExact(docID);
                            if (advance) {
                                BytesRef bytesRef = binaryDocValues.binaryValue();
                                compositeFieldValue.addFieldValue(FieldValue.newBuilder().setTextValue(bytesRef.utf8ToString()));
                            }
                            break;
                    }
                }
                //retrieve stored fields
                else if (fd.fieldType != null && fd.fieldType.stored()) {
                    String[] values = s.doc(hit.doc).getValues(name);
                    for (String fieldValue : values) {
                        compositeFieldValue.addFieldValue(FieldValue.newBuilder().setTextValue(fieldValue));
                    }
                } else {
                    Object v = doc.get(name); // FIXME: doc is never updated, not sure if this is correct
                    if (v != null) {
                        if (fd.multiValued == false) {
                            compositeFieldValue.addFieldValue(convertType(fd, v));
                        } else {
                            if (!(v instanceof List)) {
                                //FIXME: not sure this is serializable to string?
                                compositeFieldValue.addFieldValue(convertType(fd, v));
                            } else {
                                for (Object o : (List<Object>) v) {
                                    //FIXME: not sure this is serializable to string?
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
                    //FIXME: not sure this is serializable to string?
                    CompositeFieldValue value = CompositeFieldValue.newBuilder()
                            .addFieldValue(FieldValue.newBuilder()
                                    .setTextValue(v.toString())
                                    .build())
                            .build();
                    fieldValueMap.put(ent.getKey(), value);
                }
            }
        }

        return fieldValueMap;
    }

    private Map<String, CompositeFieldValue> getSortedFieldsForHit(ScoreDoc hit, Sort sort, List<String> sortFieldNames) {
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
                        //FIXME: not sure this is serializable to string?
                        value = fd.fields[i].toString();
                    }
                    var compositeFieldValue = CompositeFieldValue.newBuilder()
                            .addFieldValue(FieldValue.newBuilder().setTextValue(value))
                            .build();
                    sortedFields.put(fieldName, compositeFieldValue);
                }
            }
        }

        return sortedFields;
    }

    private void setCompositeFieldValue(CompositeFieldValue.Builder compositeFieldValue, FieldDef.FieldValueType fieldValueType, long val, boolean isSortedNumeric) {
        if (fieldValueType.equals(FieldDef.FieldValueType.DOUBLE)) {
            double value;
            if (isSortedNumeric) {
                value = NumericUtils.sortableLongToDouble(val);
            } else {
                value = Double.longBitsToDouble(val);
            }
            compositeFieldValue.addFieldValue(FieldValue.newBuilder().setDoubleValue(value));
        } else if (fieldValueType.equals(FieldDef.FieldValueType.FLOAT)) {
            float value;
            if (isSortedNumeric) {
                value = NumericUtils.sortableIntToFloat((int) val);
            } else {
                value = Float.intBitsToFloat((int) val);
            }
            compositeFieldValue.addFieldValue(FieldValue.newBuilder().setFloatValue(value));
        } else if (fieldValueType.equals(FieldDef.FieldValueType.BOOLEAN)) {
            boolean value = val == 1;
            compositeFieldValue.addFieldValue(FieldValue.newBuilder().setBooleanValue(value));
        } else if (fieldValueType.equals(FieldDef.FieldValueType.INT)) {
            compositeFieldValue.addFieldValue(FieldValue.newBuilder().setIntValue((int) val));
        } else { //LONG
            compositeFieldValue.addFieldValue(FieldValue.newBuilder().setLongValue(val));
        }

    }

    private static FieldValue convertType(FieldDef fd, Object o) {
        var fieldValue = FieldValue.newBuilder();
        if (fd.valueType == FieldDef.FieldValueType.BOOLEAN) {
            if (((Integer) o).intValue() == 1) {
                fieldValue.setBooleanValue(Boolean.TRUE);
            } else {
                assert ((Integer) o).intValue() == 0;
                fieldValue.setBooleanValue(Boolean.FALSE);
            }
        } else if (fd.valueType == FieldDef.FieldValueType.DATE_TIME) {
            fieldValue.setTextValue(msecToDateString(fd, ((Number) o).longValue()));
        } else {
            throw new IllegalArgumentException("Unable to convert object: " + o);
        }

        return fieldValue.build();
    }

    private static String msecToDateString(FieldDef fd, long value) {
        assert fd.valueType == FieldDef.FieldValueType.DATE_TIME;
        // nocommit use CTL to reuse these?
        return fd.getDateTimeParser().parser.format(new Date(value));
    }

    /**
     * Highlight configuration.
     */
    static class FieldHighlightConfig {
        /**
         * Number of passages.
         */
        public int maxPassages = -1;

        // nocommit use enum:
        /**
         * Snippet or whole.
         */
        public String mode;

        /**
         * True if field is single valued.
         */
        public boolean multiValued;

        /**
         * {@link BreakIterator} to use.
         */
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
