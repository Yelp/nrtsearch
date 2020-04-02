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

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;

import com.google.protobuf.Any;
import com.google.protobuf.ProtocolStringList;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AddDocumentHandler implements Handler<AddDocumentRequest, Any> {
    private static final Logger logger = Logger.getLogger(AddDocumentHandler.class.getName());

    @Override
    public Any handle(IndexState indexState, AddDocumentRequest addDocumentRequest) throws AddDocumentHandlerException {
        Document document = LuceneDocumentBuilder.getDocument(addDocumentRequest, indexState);
        //this is silly we dont really about about the return value here
        return Any.newBuilder().build();
    }

    static class MyField extends Field {
        public MyField(String name, FieldType ft, Object value) {
            super(name, ft);
            fieldsData = value;
        }
    }

    public static class LuceneDocumentBuilder {
        public static Document getDocument(AddDocumentRequest addDocumentRequest, IndexState indexState) throws AddDocumentHandlerException {
            Document document = new Document();
            Map<String, AddDocumentRequest.MultiValuedField> fields = addDocumentRequest.getFieldsMap();
            for (Map.Entry<String, AddDocumentRequest.MultiValuedField> entry : fields.entrySet()) {
                parseOneField(entry.getKey(), entry.getValue(), document, indexState);
            }
            return document;
        }

        /**
         * Parses a field's value, which is a MultiValuedField in all cases
         */
        private static void parseOneField(String key, AddDocumentRequest.MultiValuedField value, Document document, IndexState indexState) throws AddDocumentHandlerException {
            parseMultiValueField(indexState.getField(key), value, document);
        }

        /**
         * Parse MultiValuedField for a single field, which is always a List<String>.
         */
        private static void parseMultiValueField(FieldDef field, AddDocumentRequest.MultiValuedField value, Document document) throws AddDocumentHandlerException {
            ProtocolStringList fieldValues = value.getValueList();
            if (fieldValues.size() > 1 && !field.multiValued) {
                throw new AddDocumentHandlerException(String.format("field: %s needs to be registered as multivalued", field.name));
            }
            Object nativeValue;
            //special handling for LAT_LON field
            if (field.valueType.equals(FieldDef.FieldValueType.LAT_LON)) {
                nativeValue = getLatLonObject(field, fieldValues);
                parseOneNativeValue(field, document, nativeValue);
            } else {
                //TODO: in case of hierarchy fieldValue will be List<String> need to handle that separately (ned to enhance proto:MultiValuedField for that)
                for (String fieldValue : fieldValues) {
                    nativeValue = getNativeValue(field, fieldValue);
                    parseOneNativeValue(field, document, nativeValue);
                }
            }
        }

        private static double[] getLatLonObject(FieldDef field, ProtocolStringList fieldValues) throws AddDocumentHandlerException {
            if (fieldValues.size() != 2) {
                throw new AddDocumentHandlerException(String.format("field: %s is of type: %s and needs to be have latitude, longitude only", field.name, field.valueType));
            }
            try {
                double latitude = Double.parseDouble(fieldValues.get(0));
                double longitude = Double.parseDouble(fieldValues.get(1));
                double[] latLon = {latitude, longitude};
                return latLon;
            } catch (NumberFormatException e) {
                throw new AddDocumentHandlerException("LAT_LON field type can only take in valid latitude/longitude format", e);
            }
        }

        /**
         * Parses the current single value from MultiValuedField into the corresponding
         * java object.
         */
        private static Object getNativeValue(FieldDef fd, String value) throws AddDocumentHandlerException {
            Object o;
            switch (fd.valueType) {
                //TODO: throw errors upon invalid user values e.g. non valid doubles, int, boolean etc strings
                case TEXT:
                case ATOM:
                    o = value;
                    break;
                case BOOLEAN:
                    boolean val = Boolean.valueOf(value);
                    //convert boolean to int
                    if (val) {
                        o = Integer.valueOf(1);
                    } else {
                        o = Integer.valueOf(0);
                    }
                    break;
                case LONG:
                    o = Long.valueOf(value);
                    break;
                case INT:
                    o = Integer.valueOf(value);
                    break;
                case DOUBLE:
                    o = Double.valueOf(value);
                    break;
                case FLOAT:
                    o = Float.valueOf(value);
                    break;
                case DATE_TIME:
                    String s = value;
                    FieldDef.DateTimeParser parser = fd.getDateTimeParser();
                    parser.position.setIndex(0);
                    Date date = parser.parser.parse(s, parser.position);
                    String format = String.format("%s could not parse %s as date_time with format %s", fd.name, s, fd.dateTimeFormat, fd.dateTimeFormat);
                    if (parser.position.getErrorIndex() != -1) {
                        throw new AddDocumentHandlerException(format);
                    }
                    if (parser.position.getIndex() != s.length()) {
                        throw new AddDocumentHandlerException(format);
                    }
                    o = date.getTime();
                    break;
                default:
                    throw new AddDocumentHandlerException(String.format("%s unknown value type for field %s", fd.valueType, fd.name));
            }
            return o;
        }

        /**
         * Parses value for one field.
         */
        @SuppressWarnings({"unchecked"})
        private static void parseOneNativeValue(FieldDef fd, Document doc, Object o) throws AddDocumentHandlerException {
            assert o != null;
            assert fd != null;
            if (fd.faceted.equals(FieldDef.FacetValueType.FLAT)) {
                if (o instanceof List) {
                    throw new AddDocumentHandlerException(String.format("%s value should be String when facet=flat; got %s", fd.name, o.getClass()));
                }
                doc.add(new FacetField(fd.name, o.toString()));
            } else if (fd.faceted.equals(FieldDef.FacetValueType.HIERARCHY)) {
                //TODO: hierarchy is broken right now (We need to support List<<List<String>> within MultiValueFields in proto file
                // o is never a List in the current form
                if (o instanceof List) {
                    if (fd.multiValued) {
                        List<List<String>> values = (List<List<String>>) o;
                        for (List<String> sub : values) {
                            doc.add(new FacetField(fd.name, sub.toArray(new String[sub.size()])));
                        }
                    } else {
                        List<String> values = (List<String>) o;
                        doc.add(new FacetField(fd.name, values.toArray(new String[values.size()])));
                    }
                } else {
                    doc.add(new FacetField(fd.name, o.toString()));
                }
            } else if (fd.faceted.equals(FieldDef.FacetValueType.SORTED_SET_DOC_VALUES)) {
                if (o instanceof List) {
                    throw new AddDocumentHandlerException(String.format("%s value should be String when facet=sortedSetDocValues; got %s", fd.name, o.getClass()));
                }
                doc.add(new SortedSetDocValuesFacetField(fd.name, o.toString()));
            }

            if (fd.highlighted) {
                assert o instanceof String;
                if (fd.multiValued && (((String) o).indexOf(Constants.INFORMATION_SEP) != -1)) {
                    // TODO: we could remove this restriction if it
                    // ever matters ... we can highlight multi-valued
                    // fields at search time without stealing a
                    // character:
                    throw new AddDocumentHandlerException("%s multiValued and hihglighted fields cannot contain INFORMATION_SEPARATOR (U+001F) character: this character is used internally when highlighting multi-valued fields".format(fd.name));
                }
            }

            // Separately index doc values:
            DocValuesType dvType = fd.fieldType.docValuesType();
            if (dvType == DocValuesType.BINARY || dvType == DocValuesType.SORTED) {
                if (o instanceof String == false) {
                    throw new AddDocumentHandlerException(String.format("%s expected String but got: %s", fd.name, o));
                }
                BytesRef br = new BytesRef((String) o);
                if (fd.fieldType.docValuesType() == DocValuesType.BINARY) {
                    doc.add(new BinaryDocValuesField(fd.name, br));
                } else {
                    doc.add(new SortedDocValuesField(fd.name, br));
                }
            } else if (dvType == DocValuesType.SORTED_SET) {
                if (o instanceof List) {
                    List<String> values = (List<String>) o;
                    for (String _o : values) {
                        doc.add(new SortedSetDocValuesField(fd.name, new BytesRef(_o)));
                    }
                } else {
                    doc.add(new SortedSetDocValuesField(fd.name, new BytesRef((String) o)));
                }
            } else if (fd.valueType == FieldDef.FieldValueType.LAT_LON && dvType == DocValuesType.SORTED_NUMERIC) {
                double[] latLon = (double[]) o;
                doc.add(new LatLonDocValuesField(fd.name, latLon[0], latLon[1]));
            } else if (dvType == DocValuesType.NUMERIC || dvType == DocValuesType.SORTED_NUMERIC) {
                if (fd.valueType == FieldDef.FieldValueType.FLOAT) {
                    if (fd.multiValued) {
                        doc.add(new SortedNumericDocValuesField(fd.name, NumericUtils.floatToSortableInt(((Number) o).floatValue())));
                    } else {
                        doc.add(new FloatDocValuesField(fd.name, ((Number) o).floatValue()));
                    }
                } else if (fd.valueType == FieldDef.FieldValueType.DOUBLE) {
                    if (fd.multiValued) {
                        doc.add(new SortedNumericDocValuesField(fd.name, NumericUtils.doubleToSortableLong(((Number) o).doubleValue())));
                    } else {
                        doc.add(new DoubleDocValuesField(fd.name, ((Number) o).doubleValue()));
                    }
                } else if (fd.valueType == FieldDef.FieldValueType.INT) {
                    if (fd.multiValued) {
                        doc.add(new SortedNumericDocValuesField(fd.name, ((Number) o).intValue()));
                    } else {
                        doc.add(new NumericDocValuesField(fd.name, ((Number) o).intValue()));
                    }
                } else if (fd.valueType == FieldDef.FieldValueType.LONG || fd.valueType == FieldDef.FieldValueType.DATE_TIME) {
                    if (fd.multiValued) {
                        doc.add(new SortedNumericDocValuesField(fd.name, ((Number) o).longValue()));
                    } else {
                        doc.add(new NumericDocValuesField(fd.name, ((Number) o).longValue()));
                    }
                } else {
                    assert fd.valueType == FieldDef.FieldValueType.BOOLEAN;
                    if (fd.multiValued) {
                        doc.add(new SortedNumericDocValuesField(fd.name, ((Integer) o).intValue()));
                    } else {
                        doc.add(new NumericDocValuesField(fd.name, ((Integer) o).intValue()));
                    }
                }
            }

            // maybe add separate points field:
            if (fd.usePoints) {
                if (fd.valueType == FieldDef.FieldValueType.INT) {
                    doc.add(new IntPoint(fd.name, ((Number) o).intValue()));
                } else if (fd.valueType == FieldDef.FieldValueType.LONG || fd.valueType == FieldDef.FieldValueType.DATE_TIME) {
                    doc.add(new LongPoint(fd.name, ((Number) o).longValue()));
                } else if (fd.valueType == FieldDef.FieldValueType.FLOAT) {
                    doc.add(new FloatPoint(fd.name, ((Number) o).floatValue()));
                } else if (fd.valueType == FieldDef.FieldValueType.DOUBLE) {
                    doc.add(new DoublePoint(fd.name, ((Number) o).doubleValue()));
                } else if (fd.valueType == FieldDef.FieldValueType.LAT_LON) {
                    double[] latLon = (double[]) o;
                    doc.add(new LatLonPoint(fd.name, latLon[0], latLon[1]));
                } else {
                    throw new AssertionError(String.format("fd.usePoints=true, invalid Type specified: %s", fd.valueType));
                }
            }

            if (fd.fieldType.stored() || fd.fieldType.indexOptions() != IndexOptions.NONE) {
                // We use fieldTypeNoDV because we separately added (above) the doc values field:
                Field f = new MyField(fd.name, fd.fieldTypeNoDV, o);
                doc.add(f);
            }
        }

    }

    public static class AddDocumentHandlerException extends Handler.HandlerException {
        public AddDocumentHandlerException(String errorMessage) {
            super(errorMessage);
        }

        public AddDocumentHandlerException(String errorMessage, Throwable err) {
            super(errorMessage, err);
        }

        public AddDocumentHandlerException(Throwable err) {
            super(err);
        }

    }

    public static class DocumentIndexer implements Callable<Long> {
        private final GlobalState globalState;
        private final List<AddDocumentRequest> addDocumentRequestList;

        public DocumentIndexer(GlobalState globalState, List<AddDocumentRequest> addDocumentRequestList) {
            this.globalState = globalState;
            this.addDocumentRequestList = addDocumentRequestList;
        }

        public long runIndexingJob() throws Exception {
            logger.info(String.format("running indexing job on threadId: %s", Thread.currentThread().getName() + Thread.currentThread().getId()));
            Queue<Document> documents = new LinkedBlockingDeque<>();
            IndexState indexState = null;
            for (AddDocumentRequest addDocumentRequest : addDocumentRequestList) {
                try {
                    indexState = globalState.getIndex(addDocumentRequest.getIndexName());
                    Document document = AddDocumentHandler.LuceneDocumentBuilder.getDocument(addDocumentRequest, indexState);
                    documents.add(document);
                } catch (Exception e) {
                    logger.log(Level.WARNING, "addDocuments Cancelled", e);
                    throw new Exception(e); //parent thread should catch and send error back to client
                }
            }
            ShardState shardState = indexState.getShard(0);
            long gen;
            try {
                shardState.writer.addDocuments(new Iterable<Document>() {
                    @Override
                    public Iterator<Document> iterator() {
                        final boolean hasFacets = shardState.indexState.hasFacets();
                        int docListSize = addDocumentRequestList.size();
                        return new Iterator<Document>() {
                            private Document nextDoc;

                            @Override
                            public boolean hasNext() {
                                if (!documents.isEmpty()) {
                                    nextDoc = documents.poll();
                                    if (hasFacets) {
                                        try {
                                            nextDoc = shardState.indexState.facetsConfig.build(shardState.taxoWriter, nextDoc);
                                        } catch (IOException ioe) {
                                            throw new RuntimeException(String.format("document: %s hit exception building facets", nextDoc), ioe);
                                        }
                                    }
                                    return true;
                                } else {
                                    nextDoc = null;
                                    return false;
                                }
                            }

                            @Override
                            public Document next() {
                                return nextDoc;
                            }
                        };
                    }
                });
            } catch (IOException e) { //This exception should be caught in parent to and set responseObserver.onError(e) so client knows the job failed
                logger.log(Level.WARNING, String.format("ThreadId: %s, IndexWriter.addDocuments failed", Thread.currentThread().getName() + Thread.currentThread().getId()));
                throw new IOException(e);
            }
            logger.info(String.format("indexing job on threadId: %s done with SequenceId: %s",
                    Thread.currentThread().getName() + Thread.currentThread().getId(),
                    shardState.writer.getMaxCompletedSequenceNumber()));
            return shardState.writer.getMaxCompletedSequenceNumber();
        }

        @Override
        public Long call() throws Exception {
            return runIndexingJob();
        }
    }


}
