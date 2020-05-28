/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.yelp.nrtsearch.server.luceneserver.script;

import com.yelp.nrtsearch.server.luceneserver.doc.DocLookup;
import com.yelp.nrtsearch.server.luceneserver.doc.SegmentDocLookup;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Script to produce a double value for a given document. Implementations must have a doubleValue function,
 * which defines the script execution. The script has access to the query parameters, the document doc values
 * through {@link SegmentDocLookup}, and the document score through getScore.
 */
public abstract class ScoreScript extends DoubleValues {
    private final Map<String, Object> params;
    private final SegmentDocLookup segmentDocLookup;
    private final DoubleValues scores;
    private int docId = -1;
    private Double score;

    /**
     * ScoreScript constructor.
     * @param params script parameters from {@link com.yelp.nrtsearch.server.grpc.Script}
     * @param docLookup index level doc values lookup
     * @param leafContext lucene segment context
     * @param scores provider of segment document scores
     */
    public ScoreScript(Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext, DoubleValues scores) {
        this.params = params;
        this.segmentDocLookup = docLookup.getSegmentLookup(leafContext);
        this.scores = scores;
    }

    /**
     * Advance script to a given segment document.
     * @param doc segment doc id
     * @return if there is data for the given id, this should always be the case
     * @throws IOException
     */
    @Override
    public boolean advanceExact(int doc) throws IOException {
        segmentDocLookup.setDocId(doc);
        docId = doc;
        score = null;
        return true;
    }

    /**
     * Get the score for the current document.
     * @return document score
     * @throws IOException
     */
    public Double getScore() throws IOException {
        if (score == null) {
            scores.advanceExact(docId);
            score = scores.doubleValue();
        }
        return score;
    }

    /**
     * Get the script parameters provided in the request.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    /**
     * Get doc value lookup for this segment. During script execution, this lookup will already be advanced to
     * the current document.
     */
    public SegmentDocLookup doc() {
        return segmentDocLookup;
    }

    /**
     * Factory required from the compilation of a ScoreScript. Used to produce request level
     * {@link DoubleValuesSource}.
     */
    public interface Factory {
        /**
         * Create request level {@link DoubleValuesSource}.
         * @param params parameters from script request
         * @param docLookup index level doc value lookup provider
         * @return {@link DoubleValuesSource} to evaluate script
         */
        DoubleValuesSource newFactory(Map<String, Object> params, DocLookup docLookup);
    }

    // compile context for the ScoreScript, contains factory type info
    public static final ScriptContext<ScoreScript.Factory> CONTEXT = new ScriptContext<>("score", ScoreScript.Factory.class);

    /**
     * Simple abstract implementation of a {@link DoubleValuesSource} this can be extended for engines that need
     * to implement a custom {@link ScoreScript}. The getValues and needsScores functions must be implemented.
     * If more state is needed, the equals/hashCode should be redefined appropriately.
     *
     * This is only meant to be a helper base class. Engines are free to create there own {@link DoubleValuesSource}
     * implementations.
     */
    public static abstract class SegmentFactory extends DoubleValuesSource {
        private final Map<String, Object> params;
        private final DocLookup docLookup;

        public SegmentFactory(Map<String, Object> params, DocLookup docLookup) {
            this.params = params;
            this.docLookup = docLookup;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public DocLookup getDocLookup() {
            return docLookup;
        }

        @Override
        public DoubleValuesSource rewrite(IndexSearcher reader) {
            return this;
        }

        @Override
        public int hashCode() {
            return Objects.hash(params, docLookup);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != this.getClass()) {
                return false;
            }
            SegmentFactory factory = (SegmentFactory) obj;
            return Objects.equals(factory.params, this.params) && Objects.equals(factory.docLookup, this.docLookup);
        }

        @Override
        public String toString() {
            return "ScoreScriptDoubleValuesSource: params: " + params + ", docLookup: " + docLookup;
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            // It would be possible to cache this if we can verify no doc values for this segment have been updated
            return false;
        }
    }
}
