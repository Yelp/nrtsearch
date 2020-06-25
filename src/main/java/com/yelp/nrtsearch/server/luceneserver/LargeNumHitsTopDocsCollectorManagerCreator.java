package com.yelp.nrtsearch.server.luceneserver;

import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LargeNumHitsTopDocsCollector;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.Collection;

public class LargeNumHitsTopDocsCollectorManagerCreator {
    /**
     * Create a CollectorManager of LargeNumHitsTopDocsCollectors to facilitate parallel segment search on
     * LargeNumHitsTopDocsCollector
     */
    public static CollectorManager<LargeNumHitsTopDocsCollector, TopDocs> createSharedManager(int numHits) {
        return new CollectorManager<LargeNumHitsTopDocsCollector, TopDocs>() {

            @Override
            public LargeNumHitsTopDocsCollector newCollector() throws IOException {
                return new LargeNumHitsTopDocsCollector(numHits);
            }

            @Override
            public TopDocs reduce(Collection<LargeNumHitsTopDocsCollector> collectors) throws IOException {
                final TopDocs[] topDocs = new TopDocs[collectors.size()];
                int i = 0;
                for (LargeNumHitsTopDocsCollector collector : collectors) {
                    topDocs[i++] = collector.topDocs();
                }
                return TopDocs.merge(numHits, topDocs);
            }

        };
    }

}
