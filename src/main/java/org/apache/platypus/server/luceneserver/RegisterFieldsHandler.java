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

package org.apache.platypus.server.luceneserver;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.platypus.server.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.platypus.server.luceneserver.AnalyzerCreator.hasAnalyzer;

public class RegisterFieldsHandler implements Handler<FieldDefRequest, FieldDefResponse> {

    Logger logger = LoggerFactory.getLogger(RegisterFieldsHandler.class);
    private final JsonParser jsonParser = new JsonParser();

    /* Sets the FieldDef for every field specified in FieldDefRequest and saves it in IndexState.fields member
     * returns the String representation of the same */
    @Override
    public FieldDefResponse handle(final IndexState indexState, FieldDefRequest fieldDefRequest) throws RegisterFieldsException {
        assert indexState != null;

        final Map<String, FieldDef> pendingFieldDefs = new HashMap<>();
        final Map<String, String> saveStates = new HashMap<>();
        Set<String> seen = new HashSet<>();

        // We make two passes.  In the first pass, we do the
        // "real" fields, and second pass does the virtual
        // fields, so that any fields the virtual field
        // references are guaranteed to exist, in a single
        // request (or, from the saved json):
        for (int pass = 0; pass < 2; pass++) {
            List<Field> fields = fieldDefRequest.getFieldList();
            for (Field currentField : fields) {
                String fieldName = currentField.getName();

                if (pass == 1 && seen.contains(fieldName)) {
                    continue;
                }

//                if (!(currentField.getValue() instanceof JSONObject)) {
//                    r.fail("field \"" + fieldName + "\": expected object containing the field type but got: " + ent.getValue());
//                }

                ;
                if (pass == 0 && FieldType.VIRTUAL.equals(currentField.getType())) {
                    // Do this on 2nd pass so the field it refers to will be registered even if it's a single request
                    continue;
                }

                if (!IndexState.isSimpleName(fieldName)) {
                    throw new RegisterFieldsException("invalid field name \"" + fieldName + "\": must be [a-zA-Z_][a-zA-Z0-9]*");
                }

                if (fieldName.endsWith("_boost")) {
                    throw new RegisterFieldsException("invalid field name \"" + fieldName + "\": field names cannot end with _boost");
                }

                if (seen.contains(fieldName)) {
                    throw new RegisterFieldsException("field \"" + fieldName + "\" appears at least twice in this request");
                }

                seen.add(fieldName);

                try {
                    //convert Proto object to Json String
                    String currentFieldAsJsonString = JsonFormat.printer().print(currentField);
                    saveStates.put(fieldName, currentFieldAsJsonString);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }

                pendingFieldDefs.put(fieldName, parseOneFieldType(indexState, pendingFieldDefs, fieldName, currentField));
            }
        }

        //add FieldDef and its corresponding JsonObject to states variable in IndexState
        for (Map.Entry<String, FieldDef> ent : pendingFieldDefs.entrySet()) {
            JsonObject fieldAsJsonObject = jsonParser.parse(saveStates.get(ent.getKey())).getAsJsonObject();
            indexState.addField(ent.getValue(), fieldAsJsonObject);
        }
        String response = indexState.getAllFieldsJSON();
        FieldDefResponse reply = FieldDefResponse.newBuilder().setResponse(response).build();
        return reply;
    }

    private FieldDef parseOneFieldType(IndexState indexState, Map<String, FieldDef> pendingFieldDefs, String fieldName, Field currentField) throws RegisterFieldsException {
        FieldType fieldType = currentField.getType();
        if (FieldType.VIRTUAL.equals(currentField.getType())) {
            return parseOneVirtualFieldType(indexState, pendingFieldDefs, fieldName, currentField);
        }

        FieldDef.FieldValueType type;
        switch (fieldType) {
            case ATOM:
                type = FieldDef.FieldValueType.ATOM;
                break;
            case TEXT:
                type = FieldDef.FieldValueType.TEXT;
                break;
            case BOOLEAN:
                type = FieldDef.FieldValueType.BOOLEAN;
                break;
            case LONG:
                type = FieldDef.FieldValueType.LONG;
                break;
            case INT:
                type = FieldDef.FieldValueType.INT;
                break;
            case DOUBLE:
                type = FieldDef.FieldValueType.DOUBLE;
                break;
            case FLOAT:
                type = FieldDef.FieldValueType.FLOAT;
                break;
            case LAT_LON:
                type = FieldDef.FieldValueType.LAT_LON;
                break;
            case DATE_TIME:
                type = FieldDef.FieldValueType.DATE_TIME;
                break;
            default:
                // bug!  we declare the allowed types
                throw new AssertionError();
        }

        org.apache.lucene.document.FieldType ft = new org.apache.lucene.document.FieldType();

        // nocommit why do we have storeDocValues?  it's too low level?

        boolean dv = currentField.getStoreDocValues();
        boolean sorted = currentField.getSort();
        boolean grouped = currentField.getGroup();
        boolean stored = currentField.getStore();

        // TODO: current we only highlight using
        // PostingsHighlighter; if we enable others (that use
        // term vectors), we need to fix this so app specifies
        // up front which highlighter(s) it wants to use at
        // search time:
        boolean highlighted = currentField.getHighlight();

        if (highlighted) {
            if (type != FieldDef.FieldValueType.TEXT && type != FieldDef.FieldValueType.ATOM) {
                throw new RegisterFieldsException(String.format("field: %s cannot have highlight=true. only type=text or type=atom fields can have highlight=true", currentField.getName()));
            }
        }

        boolean multiValued = currentField.getMultiValued();
        if (multiValued) {
            // nocommit not true!  but do we need something at search time so you can pick the picker?
            //if (sorted) {
            //f.fail("multiValued", "cannot sort on multiValued fields");
            //}
            if (grouped) {
                throw new RegisterFieldsException(String.format("field: %s cannot have both group and multivalued set to true. Cannot  group on multiValued fields", currentField.getName()));
            }
        }

        // if stored was false and we are highlighting, turn it on:
        if (highlighted) {
            if (!stored) {
                stored = true;
            }
        }

        String dateTimeFormat = null;

        switch (type) {
            case TEXT:
                if (sorted) {
                    throw new RegisterFieldsException("sort: Cannot sort text fields; use atom instead");
                }
                ft.setTokenized(true);
                if (grouped) {
                    ft.setDocValuesType(DocValuesType.SORTED);
                } else if (dv) {
                    //needed to support multivalued text fields even though its not grouped
                    //since neither BINARY nor SORTED allows for multiValued fields during indexing
                    if (multiValued) {
                        ft.setDocValuesType(DocValuesType.SORTED_SET);
                    } else {
                        ft.setDocValuesType(DocValuesType.BINARY);
                    }
                }
                if (highlighted) {
                    if (stored == false) {
                        throw new RegisterFieldsException("store=false is not allowed when highlight=true");
                    }
                    ft.setStored(true);
                    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                } else {
                    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                }
                break;

            case ATOM:
                if (hasAnalyzer(currentField)) {
                    throw new RegisterFieldsException("no analyzer allowed with atom (it's hardwired to KeywordAnalyzer internally)");
                }
                if (highlighted) {
                    if (stored == false) {
                        throw new RegisterFieldsException("store=false is not allowed when highlight=true");
                    }
                    // nocommit need test highlighting atom fields
                    ft.setStored(true);
                    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                } else {
                    ft.setIndexOptions(IndexOptions.DOCS);
                }
                ft.setOmitNorms(true);
                ft.setTokenized(false);
                if (sorted || grouped) {
                    if (multiValued) {
                        ft.setDocValuesType(DocValuesType.SORTED_SET);
                    } else {
                        ft.setDocValuesType(DocValuesType.SORTED);
                    }
                } else if (grouped || dv) {
                    //needed to support multivalued text fields even though its not grouped
                    //since neither BINARY nor SORTED allows for multiValued fields during indexing
                    if (multiValued) {
                        ft.setDocValuesType(DocValuesType.SORTED_SET);
                    } else {
                        ft.setDocValuesType(DocValuesType.BINARY);
                    }
                }
                break;

            case BOOLEAN:

            case LONG:

            case INT:

            case DOUBLE:

            case FLOAT:
                if (dv || sorted || grouped) {
                    if (multiValued) {
                        ft.setDocValuesType(DocValuesType.SORTED_NUMERIC);
                    } else {
                        ft.setDocValuesType(DocValuesType.NUMERIC);
                    }
                }
                break;

            case LAT_LON:
                if (stored) {
                    throw new RegisterFieldsException("latlon fields cannot be stored");
                }
                ft.setDimensions(2, Integer.BYTES);
                if (sorted) {
                    ft.setDocValuesType(DocValuesType.SORTED_NUMERIC);
                }
                break;

            case DATE_TIME:

                // nocommit maybe we only accept https://www.ietf.org/rfc/rfc3339.txt

                dateTimeFormat = currentField.getDateTimeFormat();

                // make sure the format is valid:
                try {
                    new SimpleDateFormat(dateTimeFormat);
                } catch (IllegalArgumentException iae) {
                    throw new RegisterFieldsException("dateTimeFormat could not parse pattern", iae);
                }
                if (grouped) {
                    throw new RegisterFieldsException("cannot group on datetime fields");
                }
                if (dv || sorted) {
                    if (multiValued) {
                        ft.setDocValuesType(DocValuesType.SORTED_NUMERIC);
                    } else {
                        ft.setDocValuesType(DocValuesType.NUMERIC);
                    }
                }

                break;

            default:
                throw new AssertionError("unhandled type \"" + type + "\"");
        }

        // nocommit InetAddressPoint, BiggishInteger

        if (stored == Boolean.TRUE) {
            ft.setStored(true);
        }

        boolean usePoints = false;

        if (currentField.getSearch()) {
            if (type == FieldDef.FieldValueType.INT ||
                    type == FieldDef.FieldValueType.LONG ||
                    type == FieldDef.FieldValueType.FLOAT ||
                    type == FieldDef.FieldValueType.DOUBLE ||
                    type == FieldDef.FieldValueType.LAT_LON ||
                    type == FieldDef.FieldValueType.DATE_TIME) {
                usePoints = true;
            } else if (ft.indexOptions() == IndexOptions.NONE) {
                ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            }
        } else if (highlighted) {
            throw new RegisterFieldsException("search must be true when highlight is true");
        }

        if (hasAnalyzer(currentField) && ft.indexOptions() == IndexOptions.NONE) {
            throw new RegisterFieldsException("no analyzer allowed when search=false");
        }

        if (type == FieldDef.FieldValueType.TEXT || type == FieldDef.FieldValueType.ATOM) {

            if (ft.indexOptions() != IndexOptions.NONE) {
                ft.setTokenized(currentField.getTokenize());
                ft.setOmitNorms(currentField.getOmitNorms());

                if (!currentField.getTermVectors().equals(TermVectors.NO_TERMVECTORS)) {
                    if (currentField.getTermVectors().equals(TermVectors.TERMS)) {
                        ft.setStoreTermVectors(true);
                    } else if (currentField.getTermVectors().equals(TermVectors.TERMS_POSITIONS)) {
                        ft.setStoreTermVectors(true);
                        ft.setStoreTermVectorPositions(true);
                    } else if (currentField.getTermVectors().equals(TermVectors.TERMS_POSITIONS_OFFSETS)) {
                        ft.setStoreTermVectors(true);
                        ft.setStoreTermVectorPositions(true);
                        ft.setStoreTermVectorOffsets(true);
                    } else if (currentField.getTermVectors().equals(TermVectors.TERMS_POSITIONS_OFFSETS_PAYLOADS)) {
                        ft.setStoreTermVectors(true);
                        ft.setStoreTermVectorPositions(true);
                        ft.setStoreTermVectorOffsets(true);
                        ft.setStoreTermVectorPayloads(true);
                    } else {
                        assert false;
                    }
                }

                if (currentField.getIndexOptions().equals(org.apache.platypus.server.grpc.IndexOptions.DOCS)) {
                    ft.setIndexOptions(IndexOptions.DOCS);
                } else if (currentField.getIndexOptions().equals(org.apache.platypus.server.grpc.IndexOptions.DOCS_FREQS)) {
                    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
                } else if (currentField.getIndexOptions().equals(org.apache.platypus.server.grpc.IndexOptions.DOCS_FREQS_POSITIONS)) {
                    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                } else if (currentField.getIndexOptions().equals(org.apache.platypus.server.grpc.IndexOptions.DOCS_FREQS_POSITIONS_OFFSETS)) {
                    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                } else { //default option
                    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                }
            }
        } else if (type == FieldDef.FieldValueType.BOOLEAN) {
            ft.setOmitNorms(true);
            ft.setTokenized(false);
            ft.setIndexOptions(IndexOptions.DOCS);
        }

        // nocommit open up binary points too
        // nocommit open up multi-dimensional points

        String pf = currentField.getPostingsFormat().isEmpty() ? ServerCodec.DEFAULT_POSTINGS_FORMAT : currentField.getPostingsFormat();
        if (PostingsFormat.forName(pf) == null) {
            throw new RegisterFieldsException("unrecognized postingsFormat \"" + pf + "\"");
        }
        String dvf = currentField.getPostingsFormat().isEmpty() ? ServerCodec.DEFAULT_DOC_VALUES_FORMAT : currentField.getDocValuesFormat();
        if (DocValuesFormat.forName(dvf) == null) {
            throw new RegisterFieldsException("unrecognized docValuesFormat \"" + dvf + "\"");
        }

        Similarity sim;
        if (!currentField.getSimilarity().isEmpty()) {
            String similarityStr = currentField.getSimilarity();
            if (similarityStr.equalsIgnoreCase("DefaultSimilarity")) {
                sim = new ClassicSimilarity();
            } else if (similarityStr.equalsIgnoreCase("BM25Similarity")) {
                //TODO pass custom k1, b values so that we can call Constructor BM25Similarity(float k1, float b)
                sim = new BM25Similarity();
            } else {
                //TODO support CustomSimilarity
                assert false;
                sim = null;
            }
        } else {
            sim = new BM25Similarity();
        }

        Analyzer indexAnalyzer;
        Analyzer searchAnalyzer;
        boolean isIndexedTextField = type == FieldDef.FieldValueType.TEXT && ft.indexOptions() != IndexOptions.NONE;

        if (currentField.getAnalyzer() != null) {
            // If the analyzer field is provided, use it to get an analyzer to use for both indexing and search
            Analyzer analyzer = AnalyzerCreator.getAnalyzer(currentField.getAnalyzer());
            indexAnalyzer = searchAnalyzer = analyzer;
        } else {
            // Analyzer field is absent in request - set index and search analyzers individually

            if (currentField.getIndexAnalyzer() != null) {
                // Index analyzer was provided, use it to create an analyzer.
                indexAnalyzer = AnalyzerCreator.getAnalyzer(currentField.getIndexAnalyzer());
            } else if (isIndexedTextField) {
                // If no index analyzer is provided for a text field that will be indexed (have doc values), use the
                // StandardAnalyzer.
                indexAnalyzer = AnalyzerCreator.getStandardAnalyzer();
            } else {
                // No index analyzer was found or needed. Use the dummy analyzer.
                indexAnalyzer = dummyAnalyzer;
            }

            if (currentField.getSearchAnalyzer() != null) {
                // Search analyzer was provided, use it to create an analyzer.
                searchAnalyzer = AnalyzerCreator.getAnalyzer(currentField.getSearchAnalyzer());
            } else if (isIndexedTextField) {
                // If no search analyzer is provided for a text field that will be indexed (have doc values), use the
                // StandardAnalyzer.
                searchAnalyzer = AnalyzerCreator.getStandardAnalyzer();
            } else {
                // No search analyzer was found or needed. Use the index analyzer which may be a valid analyzer or
                // the dummyAnalyzer.
                searchAnalyzer = indexAnalyzer;
            }
        }

        // TODO: facets w/ dates
        FacetType facetType = currentField.getFacet();
        FieldDef.FacetValueType facetValueType = FieldDef.FacetValueType.NO_FACETS;
        if (facetType.equals(FacetType.HIERARCHY)) {
            facetValueType = FieldDef.FacetValueType.HIERARCHY;
            if (highlighted) {
                throw new RegisterFieldsException("facet=hierarchy fields cannot have highlight=true");
            }
            if (ft.indexOptions() != IndexOptions.NONE) {
                throw new RegisterFieldsException("facet=hierarchy fields cannot have search=true");
            }
            if (ft.stored()) {
                throw new RegisterFieldsException("facet=hierarchy fields cannot have store=true");
            }
        } else if (facetType.equals(FacetType.NUMERIC_RANGE)) {
            facetValueType = FieldDef.FacetValueType.NUMERIC_RANGE;
            if (type != FieldDef.FieldValueType.LONG && type != FieldDef.FieldValueType.INT
                    && type != FieldDef.FieldValueType.FLOAT && type != FieldDef.FieldValueType.DOUBLE) {
                throw new RegisterFieldsException("numericRange facets only applies to numeric types");
            }
            if (ft.indexOptions() == IndexOptions.NONE && usePoints == false) {
                throw new RegisterFieldsException("facet=numericRange fields must have search=true");
            }
            // We index the field as points, for drill-down, and store doc values, for dynamic facet counting
            ft.setDocValuesType(DocValuesType.NUMERIC);
        } else if (facetType.equals(FacetType.NO_FACETS)) {
            facetValueType = FieldDef.FacetValueType.NO_FACETS;
            if (ft.indexOptions() == IndexOptions.NONE && ft.stored() == false
                    && ft.docValuesType() == DocValuesType.NONE && usePoints == false) {
                throw new RegisterFieldsException(String.format("field %s does nothing: it's neither searched, stored, sorted, grouped, highlighted nor faceted", fieldName));
            }
        }

        ft.freeze();

        if (facetType.equals(FacetType.NO_FACETS) == false && facetType.equals(FacetType.NUMERIC_RANGE) == false) {
            // hierarchy, float or sortedSetDocValues
            if (facetType.equals(FacetType.HIERARCHY)) {
                indexState.facetsConfig.setHierarchical(fieldName, true);
            }
            if (multiValued) {
                indexState.facetsConfig.setMultiValued(fieldName, true);
            }
            indexState.facetsConfig.setIndexFieldName(fieldName, currentField.getFacetIndexFieldName());
        }

        // nocommit facetsConfig.setRequireDimCount
        logger.info("REGISTER: " + fieldName + " -> " + ft);

        return new FieldDef(fieldName, ft, type, facetValueType, pf, dvf, multiValued, usePoints, sim, indexAnalyzer, searchAnalyzer, highlighted, null, dateTimeFormat);

    }

    /**
     * Messy: we need this for indexed-but-not-tokenized
     * fields, solely for .getOffsetGap I think.
     */
    public final static Analyzer dummyAnalyzer = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            throw new UnsupportedOperationException();
        }
    };

    private FieldDef parseOneVirtualFieldType(IndexState indexState, Map<String, FieldDef> pendingFieldDefs, String fieldName, Field currentField) throws RegisterFieldsException {
        String exprString = currentField.getExpression();
        Expression expr;
        try {
            expr = JavascriptCompiler.compile(exprString);
        } catch (ParseException pe) {
            // Static error (e.g. bad JavaScript syntax):
            throw new RegisterFieldsException(String.format("could not parse expression: %s", exprString), pe);
        }
        Map<String, FieldDef> allFields = new HashMap<>(indexState.getAllFields());
        allFields.putAll(pendingFieldDefs);

        DoubleValuesSource values;
        values = expr.getDoubleValuesSource(new FieldDefBindings(allFields));

        return new FieldDef(fieldName, null, FieldDef.FieldValueType.VIRTUAL, null, null, null, true, false, null, null, null, false, values, null);

    }

    public static class RegisterFieldsException extends Handler.HandlerException {
        public RegisterFieldsException(String errorMessage) {
            super(errorMessage);
        }

        public RegisterFieldsException(String errorMessage, Throwable err) {
            super(errorMessage, err);
        }


    }
}
