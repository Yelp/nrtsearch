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

import java.io.IOException;
import java.util.List;

/**
 * This is sadly necessary because for ToParentBlockJoinQuery we must invoke .scorer not .bulkScorer, yet for DrillSideways we must do
 * exactly the opposite!
 */

public class MyIndexSearcher extends IndexSearcher {
    public MyIndexSearcher(IndexReader reader) {
        super(reader);
    }

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
