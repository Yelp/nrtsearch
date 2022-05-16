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

import com.google.gson.JsonParser;
import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.SettingsRequest;
import com.yelp.nrtsearch.server.grpc.SettingsResponse;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Sortable;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import com.yelp.nrtsearch.server.luceneserver.index.LegacyIndexState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.packed.PackedInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SettingsHandler implements Handler<SettingsRequest, SettingsResponse> {
  Logger logger = LoggerFactory.getLogger(SettingsHandler.class);
  private final JsonParser jsonParser = new JsonParser();

  @Override
  public SettingsResponse handle(final IndexState indexStateIn, SettingsRequest settingsRequest)
      throws SettingsHandlerException {
    if (!(indexStateIn instanceof LegacyIndexState)) {
      return handleAsSettingsV2(indexStateIn, settingsRequest);
    }
    LegacyIndexState indexState = (LegacyIndexState) indexStateIn;
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
        throw new SettingsHandlerException(
            "concurrentMergeScheduler.maxThreadCount must also specify concurrentMergeScheduler.maxMergeCount");
      }
    } else if (settingsRequest.getConcurrentMergeSchedulerMaxMergeCount() != 0) {
      throw new SettingsHandlerException(
          "concurrentMergeScheduler.maxThreadCount must also specify concurrentMergeScheduler.maxThreadCount");
    }

    // TODO pass in this parameter as a part of normsFormat field instead??
    float acceptableOverheadRatio = PackedInts.FASTEST;
    indexState.setNormsFormat(settingsRequest.getNormsFormat(), acceptableOverheadRatio);

    if (!settingsRequest.getIndexSort().getSortedFieldsList().isEmpty()) {
      SortFields sortedFields = settingsRequest.getIndexSort();
      logger.info(
          String.format(
              "Creating SortFields for fields: %s",
              Arrays.toString(sortedFields.getSortedFieldsList().toArray())));
      Sort sort = parseSort(indexState, sortedFields.getSortedFieldsList(), null, null);
      String sortedFeldsAsString;
      // Convert ProtoBuff object to a String and later using jsonParser to JsonObject.
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

  private SettingsResponse handleAsSettingsV2(
      IndexState indexState, SettingsRequest settingsRequest) throws SettingsHandlerException {
    IndexStateManager indexStateManager;
    try {
      indexStateManager = indexState.getGlobalState().getIndexStateManager(indexState.getName());
    } catch (IOException e) {
      throw new SettingsHandlerException("Unable to get index state manager", e);
    }

    IndexSettings.Builder indexSettingsBuilder = IndexSettings.newBuilder();

    if (!settingsRequest.getDirectory().isEmpty()) {
      indexSettingsBuilder.setDirectory(
          StringValue.newBuilder().setValue(settingsRequest.getDirectory()).build());
    }
    if (settingsRequest.getConcurrentMergeSchedulerMaxThreadCount() != 0) {
      indexSettingsBuilder.setConcurrentMergeSchedulerMaxThreadCount(
          Int32Value.newBuilder()
              .setValue(settingsRequest.getConcurrentMergeSchedulerMaxThreadCount())
              .build());
    }
    if (settingsRequest.getConcurrentMergeSchedulerMaxMergeCount() != 0) {
      indexSettingsBuilder.setConcurrentMergeSchedulerMaxMergeCount(
          Int32Value.newBuilder()
              .setValue(settingsRequest.getConcurrentMergeSchedulerMaxMergeCount())
              .build());
    }
    if (settingsRequest.hasIndexSort()) {
      indexSettingsBuilder.setIndexSort(settingsRequest.getIndexSort());
    }
    indexSettingsBuilder.setIndexMergeSchedulerAutoThrottle(
        BoolValue.newBuilder()
            .setValue(settingsRequest.getIndexMergeSchedulerAutoThrottle())
            .build());
    indexSettingsBuilder.setNrtCachingDirectoryMaxSizeMB(
        DoubleValue.newBuilder()
            .setValue(settingsRequest.getNrtCachingDirectoryMaxSizeMB())
            .build());
    indexSettingsBuilder.setNrtCachingDirectoryMaxMergeSizeMB(
        DoubleValue.newBuilder()
            .setValue(settingsRequest.getNrtCachingDirectoryMaxMergeSizeMB())
            .build());

    IndexSettings updatedSettings;
    try {
      updatedSettings = indexStateManager.updateSettings(indexSettingsBuilder.build());
    } catch (IOException e) {
      throw new SettingsHandlerException("Unable to update index settings", e);
    }

    String settingsStr;
    try {
      settingsStr = JsonFormat.printer().print(updatedSettings);
    } catch (IOException e) {
      throw new SettingsHandlerException("Unable to print updated settings to json", e);
    }
    return SettingsResponse.newBuilder().setResponse(settingsStr).build();
  }

  /** Decodes a list of SortType into the corresponding Sort. */
  static Sort parseSort(
      IndexState state,
      List<SortType> fields,
      List<String> sortFieldNames,
      Map<String, FieldDef> dynamicFields)
      throws SettingsHandlerException {
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
          throw new SettingsHandlerException(
              "field \""
                  + fieldName
                  + "\" was not registered and was not specified as a dynamicField");
        }

        if (!(fd instanceof Sortable)) {
          throw new SettingsHandlerException(
              String.format("field: %s does not support sorting", fieldName));
        }
        sf = ((Sortable) fd).getSortField(_sub);
      }
      sortFields.add(sf);
    }

    return new Sort(sortFields.toArray(new SortField[0]));
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
