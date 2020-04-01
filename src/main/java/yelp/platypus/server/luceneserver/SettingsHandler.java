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

package yelp.platypus.server.luceneserver;

import yelp.platypus.server.grpc.Point;
import yelp.platypus.server.grpc.Selector;
import yelp.platypus.server.grpc.SettingsRequest;
import yelp.platypus.server.grpc.SettingsResponse;
import yelp.platypus.server.grpc.SortFields;
import yelp.platypus.server.grpc.SortType;

import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.*;
import org.apache.lucene.util.packed.PackedInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsHandler implements Handler<SettingsRequest, SettingsResponse> {
    Logger logger = LoggerFactory.getLogger(SettingsHandler.class);
    private final JsonParser jsonParser = new JsonParser();

    @Override
    public SettingsResponse handle(final IndexState indexState, SettingsRequest settingsRequest) throws SettingsHandlerException {
        // nocommit how to / should we make this truly thread
        // safe?
        final DirectoryFactory df;
        final String directoryJSON;
        if (!settingsRequest.getDirectory().isEmpty()) {
            directoryJSON = settingsRequest.getDirectory();
            df = DirectoryFactory.get(settingsRequest.getDirectory());
        } else {
            df = null;
            directoryJSON = null;
        }

        // make sure both or none of the CMS thread settings are set
        if (settingsRequest.getConcurrentMergeSchedulerMaxThreadCount() != 0) {
            if (settingsRequest.getConcurrentMergeSchedulerMaxMergeCount() != 0) {
                // ok
            } else {
                throw new SettingsHandlerException("concurrentMergeScheduler.maxThreadCount must also specify concurrentMergeScheduler.maxMergeCount");
            }
        } else if (settingsRequest.getConcurrentMergeSchedulerMaxMergeCount() != 0) {
            throw new SettingsHandlerException("concurrentMergeScheduler.maxThreadCount must also specify concurrentMergeScheduler.maxThreadCount");
        }

        //TODO pass in this parameter as a part of normsFormat field instead??
        float acceptableOverheadRatio = PackedInts.FASTEST;
        indexState.setNormsFormat(settingsRequest.getNormsFormat(), acceptableOverheadRatio);

        if (!settingsRequest.getIndexSort().getSortedFieldsList().isEmpty()) {
            SortFields sortedFields = settingsRequest.getIndexSort();
            logger.info(String.format("Creating SortFields for fields: %s", Arrays.toString(sortedFields.getSortedFieldsList().toArray())));
            Sort sort = parseSort(indexState, sortedFields.getSortedFieldsList(), null, null);
            String sortedFeldsAsString;
            //Convert ProtoBuff object to a String and later using jsonParser to JsonObject.
            try {
                sortedFeldsAsString = JsonFormat.printer().print(sortedFields);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            indexState.setIndexSort(sort, jsonParser.parse(sortedFeldsAsString).getAsJsonObject());
        }

        // nocommit these settings take effect even if there is
        // an error?
        indexState.mergeSimpleSettings(settingsRequest);

        if (df != null) {
            indexState.setDirectoryFactory(df, directoryJSON);
        }

        String response = indexState.getSettingsJSON();
        SettingsResponse reply = SettingsResponse.newBuilder().setResponse(response).build();
        return reply;
    }

    /**
     * Decodes a list of SortType into the corresponding Sort.
     */
    static Sort parseSort(IndexState state, List<SortType> fields, List<String> sortFieldNames, Map<String, FieldDef> dynamicFields) throws SettingsHandlerException {
        List<SortField> sortFields = new ArrayList<>();
        for (SortType _sub : fields) {
            String fieldName = _sub.getFieldName();
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
                    throw new SettingsHandlerException("field \"" + fieldName + "\" was not registered and was not specified as a dynamicField");
                }
                if (fd.valueSource != null) {
                    sf = fd.valueSource.getSortField(_sub.getReverse());
                } else if (fd.valueType == FieldDef.FieldValueType.LAT_LON) {
                    if (fd.fieldType.docValuesType() == DocValuesType.NONE) {
                        throw new SettingsHandlerException("field \"" + fieldName + "\" was not registered with sort=true");
                    }
                    Point origin = _sub.getOrigin();
                    sf = LatLonDocValuesField.newDistanceSort(fieldName, origin.getLatitude(), origin.getLongitude());
                } else {
                    if ((fd.fieldType != null && fd.fieldType.docValuesType() == DocValuesType.NONE) ||
                            (fd.fieldType == null && fd.valueSource == null)) {
                        throw new SettingsHandlerException("field \"" + fieldName + "\" was not registered with sort=true");
                    }

                    if (fd.multiValued) {
                        Selector selector = _sub.getSelector();
                        if (fd.valueType == FieldDef.FieldValueType.ATOM) {
                            SortedSetSelector.Type sortedSelector;
                            if (selector.equals(Selector.MIN)) {
                                sortedSelector = SortedSetSelector.Type.MIN;
                            } else if (selector.equals(Selector.MAX)) {
                                sortedSelector = SortedSetSelector.Type.MAX;
                            } else if (selector.equals(Selector.MIDDLE_MIN)) {
                                sortedSelector = SortedSetSelector.Type.MIDDLE_MIN;
                            } else if (selector.equals(Selector.MIDDLE_MAX)) {
                                sortedSelector = SortedSetSelector.Type.MIDDLE_MAX;
                            } else {
                                assert false;
                                // dead code but javac disagrees
                                sortedSelector = null;
                            }
                            sf = new SortedSetSortField(fieldName, _sub.getReverse(), sortedSelector);
                        } else if (fd.valueType == FieldDef.FieldValueType.INT) {
                            sf = new SortedNumericSortField(fieldName, SortField.Type.INT, _sub.getReverse(), parseNumericSelector(selector));
                        } else if (fd.valueType == FieldDef.FieldValueType.LONG) {
                            sf = new SortedNumericSortField(fieldName, SortField.Type.LONG, _sub.getReverse(), parseNumericSelector(selector));
                        } else if (fd.valueType == FieldDef.FieldValueType.FLOAT) {
                            sf = new SortedNumericSortField(fieldName, SortField.Type.FLOAT, _sub.getReverse(), parseNumericSelector(selector));
                        } else if (fd.valueType == FieldDef.FieldValueType.DOUBLE) {
                            sf = new SortedNumericSortField(fieldName, SortField.Type.DOUBLE, _sub.getReverse(), parseNumericSelector(selector));
                        } else {
                            throw new SettingsHandlerException("field cannot sort by multiValued field \"" + fieldName + "\": type is " + fd.valueType);
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
                            throw new SettingsHandlerException("field cannot sort by field \"" + fieldName + "\": type is " + fd.valueType);
                        }

                        sf = new SortField(fieldName,
                                sortType,
                                _sub.getReverse());
                    }
                }

                boolean missingLast = _sub.getMissingLat();

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
                } else if (missingLast == true) {
                    throw new SettingsHandlerException("missingLast field=" + fieldName + ": can only specify missingLast for string and numeric field types: got SortField type " + sf.getType());
                }
            }
            sortFields.add(sf);
        }

        return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
    }

    private static SortedNumericSelector.Type parseNumericSelector(Selector selector) throws SettingsHandlerException {
        if (selector.equals(Selector.MIN)) {
            return SortedNumericSelector.Type.MIN;
        } else if (selector.equals(Selector.MAX)) {
            return SortedNumericSelector.Type.MAX;
        } else {
            throw new SettingsHandlerException("selector must be min or max for multi-valued numeric sort fields");
        }
    }

    public static class SettingsHandlerException extends HandlerException {
        public SettingsHandlerException(String errorMessage) {
            super(errorMessage);
        }

        public SettingsHandlerException(String errorMessage, Throwable err) {
            super(errorMessage, err);
        }


    }


}
