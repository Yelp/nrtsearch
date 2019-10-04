/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.platypus.server;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene80.Lucene80Codec;


/**
 * Implements per-index {@link Codec}.
 */

public class ServerCodec extends Lucene80Codec {

    public final static String DEFAULT_POSTINGS_FORMAT = "Lucene50";
    public final static String DEFAULT_DOC_VALUES_FORMAT = "Lucene80";

    private final IndexState state;
    // nocommit expose compression control

    /**
     * Sole constructor.
     */
    public ServerCodec(IndexState state) {
        this.state = state;
    }

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
        String pf;
        try {
            pf = state.getField(field).postingsFormat;
        } catch (IllegalArgumentException iae) {
            // The indexed facets field will have drill-downs,
            // which will pull the postings format:
            if (state.internalFacetFieldNames.contains(field)) {
                return super.getPostingsFormatForField(field);
            } else {
                throw iae;
            }
        }
        return PostingsFormat.forName(pf);
    }

    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
        String dvf;
        try {
            dvf = state.getField(field).docValuesFormat;
        } catch (IllegalArgumentException iae) {
            if (state.internalFacetFieldNames.contains(field)) {
                return super.getDocValuesFormatForField(field);
            } else {
                throw iae;
            }
        }
        return DocValuesFormat.forName(dvf);
    }
}

