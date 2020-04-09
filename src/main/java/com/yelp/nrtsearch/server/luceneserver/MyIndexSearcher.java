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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NamedThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This is sadly necessary because for ToParentBlockJoinQuery we must invoke .scorer not .bulkScorer, yet for DrillSideways we must do
 * exactly the opposite!
 */

public class MyIndexSearcher extends IndexSearcher {
    /**
     * Thresholds for index slice allocation logic. To change the default, extend
     * <code> IndexSearcher</code> and use custom values
     * TODO: convert these to configs?
     */
    private static final int MAX_DOCS_PER_SLICE = 250_000;
    private static final int MAX_SEGMENTS_PER_SLICE = 5;

    private static Executor getSearchExecutor() {
        int MAX_SEARCHING_THREADS = Runtime.getRuntime().availableProcessors();
        int MAX_BUFFERED_ITEMS = Math.max(100, 2 * MAX_SEARCHING_THREADS);
        // Seems to be substantially faster than ArrayBlockingQueue at high throughput:
        final BlockingQueue<Runnable> docsToIndex = new LinkedBlockingQueue<Runnable>(MAX_BUFFERED_ITEMS);
        //same as Executors.newFixedThreadPool except we want a NamedThreadFactory instead of defaultFactory
        return new ThreadPoolExecutor(MAX_SEARCHING_THREADS,
                MAX_SEARCHING_THREADS,
                0, TimeUnit.SECONDS,
                docsToIndex,
                new NamedThreadFactory("LuceneSearchExecutor"));
    }

    public MyIndexSearcher(IndexReader reader) {
        super(reader, getSearchExecutor());
    }

    /*** start segment to thread mapping **/
    protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
        return slices(leaves, MAX_DOCS_PER_SLICE, MAX_SEGMENTS_PER_SLICE);
    }

     /* Better Segment To Thread Mapping Algorithm: https://issues.apache.org/jira/browse/LUCENE-8757
     This change is available in 9.0 (master) which is not released yet
     https://github.com/apache/lucene-solr/blob/master/lucene/core/src/java/org/apache/lucene/search/IndexSearcher.java#L316
     We can remove this method once luceneVersion is updated to 9.x
     * */

    /**
     * Static method to segregate LeafReaderContexts amongst multiple slices
     */
    public static LeafSlice[] slices(List<LeafReaderContext> leaves, int maxDocsPerSlice,
                                     int maxSegmentsPerSlice) {
        // Make a copy so we can sort:
        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);

        // Sort by maxDoc, descending:
        Collections.sort(sortedLeaves,
                Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

        final List<List<LeafReaderContext>> groupedLeaves = new ArrayList<>();
        long docSum = 0;
        List<LeafReaderContext> group = null;
        for (LeafReaderContext ctx : sortedLeaves) {
            if (ctx.reader().maxDoc() > maxDocsPerSlice) {
                assert group == null;
                groupedLeaves.add(Collections.singletonList(ctx));
            } else {
                if (group == null) {
                    group = new ArrayList<>();
                    group.add(ctx);

                    groupedLeaves.add(group);
                } else {
                    group.add(ctx);
                }

                docSum += ctx.reader().maxDoc();
                if (group.size() >= maxSegmentsPerSlice || docSum > maxDocsPerSlice) {
                    group = null;
                    docSum = 0;
                }
            }
        }

        LeafSlice[] slices = new LeafSlice[groupedLeaves.size()];
        int upto = 0;
        for (List<LeafReaderContext> currentLeaf : groupedLeaves) {
            //LeafSlice constructor has changed in 9.x. This allows to use old constructor.
            Collections.sort(currentLeaf, Comparator.comparingInt(l -> l.docBase));
            LeafReaderContext[] leavesArr = currentLeaf.toArray(new LeafReaderContext[0]);
            slices[upto] = new LeafSlice(leavesArr);
            ++upto;
        }

        return slices;
    }

    /*** end segment to thread mapping **/

    @Override
    protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        for (LeafReaderContext ctx : leaves) { // search each subreader
            // we force the use of Scorer (not BulkScorer) to make sure
            // that the scorer passed to LeafCollector.setScorer supports
            // Scorer.getChildren
            final LeafCollector leafCollector = collector.getLeafCollector(ctx);
            if (weight.getQuery().toString().contains("DrillSidewaysQuery")) {
                BulkScorer scorer = weight.bulkScorer(ctx);
                if (scorer != null) {
                    try {
                        scorer.score(leafCollector, ctx.reader().getLiveDocs());
                    } catch (CollectionTerminatedException e) {
                        // collection was terminated prematurely
                        // continue with the following leaf
                    }
                }
            } else {
                Scorer scorer = weight.scorer(ctx);
                if (scorer != null) {
                    leafCollector.setScorer(scorer);
                    final Bits liveDocs = ctx.reader().getLiveDocs();
                    final DocIdSetIterator it = scorer.iterator();
                    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                        if (liveDocs == null || liveDocs.get(doc)) {
                            leafCollector.collect(doc);
                        }
                    }
                }
            }
        }
    }
}
