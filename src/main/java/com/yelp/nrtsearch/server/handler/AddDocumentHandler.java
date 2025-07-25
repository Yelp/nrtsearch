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
package com.yelp.nrtsearch.server.handler;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ProtocolStringList;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.IdFieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.field.properties.DocValueUpdatable;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.AddDocumentResponse;
import com.yelp.nrtsearch.server.grpc.DeadlineUtils;
import com.yelp.nrtsearch.server.grpc.FacetHierarchyPath;
import com.yelp.nrtsearch.server.grpc.IndexingRequestType;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.monitoring.IndexingMetrics;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddDocumentHandler extends Handler<AddDocumentRequest, AddDocumentResponse> {
  private static final Logger logger = LoggerFactory.getLogger(AddDocumentHandler.class);

  public AddDocumentHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public StreamObserver<AddDocumentRequest> handle(
      StreamObserver<AddDocumentResponse> responseObserver) {
    return new StreamObserver<>() {
      final Multimap<String, Future<Long>> futures = HashMultimap.create();
      // Map of {indexName: addDocumentRequestQueue}
      final Map<String, ArrayBlockingQueue<AddDocumentRequest>> addDocumentRequestQueueMap =
          new ConcurrentHashMap<>();
      // Map of {indexName: count}
      final Map<String, Long> countMap = new ConcurrentHashMap<>();

      private int getAddDocumentsMaxBufferLen(String indexName) {
        try {
          return getGlobalState().getIndexOrThrow(indexName).getAddDocumentsMaxBufferLen();
        } catch (Exception e) {
          String error =
              String.format("Index %s does not exist, unable to add documents", indexName);
          logger.error(error, e);
          throw Status.INVALID_ARGUMENT.withDescription(error).withCause(e).asRuntimeException();
        }
      }

      private ArrayBlockingQueue<AddDocumentRequest> getAddDocumentRequestQueue(String indexName) {
        if (addDocumentRequestQueueMap.containsKey(indexName)) {
          return addDocumentRequestQueueMap.get(indexName);
        } else {
          int addDocumentsMaxBufferLen = getAddDocumentsMaxBufferLen(indexName);
          ArrayBlockingQueue<AddDocumentRequest> addDocumentRequestQueue =
              new ArrayBlockingQueue<>(addDocumentsMaxBufferLen);
          addDocumentRequestQueueMap.put(indexName, addDocumentRequestQueue);
          return addDocumentRequestQueue;
        }
      }

      private void processIndexDocument(AddDocumentRequest request, String indexName) {
        ArrayBlockingQueue<AddDocumentRequest> addDocumentRequestQueue;
        try {
          addDocumentRequestQueue = getAddDocumentRequestQueue(indexName);
        } catch (Exception e) {
          onError(e);
          return;
        }
        logger.debug(
            String.format(
                "onNext, index: %s, addDocumentRequestQueue size: %s",
                indexName, addDocumentRequestQueue.size()));
        incrementCount(indexName);
        addDocumentRequestQueue.add(request);
        if (addDocumentRequestQueue.remainingCapacity() == 0) {
          logger.debug(
              String.format(
                  "indexing addDocumentRequestQueue size: %s, total: %s",
                  addDocumentRequestQueue.size(), getCount(indexName)));
          try {
            DeadlineUtils.checkDeadline("addDocuments: onNext", "INDEXING");
            List<AddDocumentRequest> addDocRequestList = new ArrayList<>(addDocumentRequestQueue);
            Future<Long> future =
                getGlobalState()
                    .submitIndexingTask(
                        Context.current()
                            .wrap(
                                new DocumentIndexer(
                                    getGlobalState(), addDocRequestList, indexName)));
            futures.put(indexName, future);
          } catch (Exception e) {
            responseObserver.onError(e);
          } finally {
            addDocumentRequestQueue.clear();
          }
        }
      }

      private long getCount(String indexName) {
        return countMap.getOrDefault(indexName, 0L);
      }

      private void incrementCount(String indexName) {
        if (countMap.containsKey(indexName)) {
          countMap.put(indexName, countMap.get(indexName) + 1);
        } else {
          countMap.put(indexName, 1L);
        }
      }

      @Override
      public void onNext(AddDocumentRequest addDocumentRequest) {
        String indexName = addDocumentRequest.getIndexName();
        int indexNamesCount = addDocumentRequest.getIndexNamesCount();
        if (indexName.isEmpty() && indexNamesCount == 0) {
          onError(
              Status.INVALID_ARGUMENT
                  .withDescription(
                      "Must provide exactly one of indexName or indexNames but neither is set")
                  .asRuntimeException());
        } else if (!indexName.isEmpty() && indexNamesCount > 0) {
          onError(
              Status.INVALID_ARGUMENT
                  .withDescription(
                      "Must provide exactly one of indexName or indexNames but both are set")
                  .asRuntimeException());
        } else if (indexNamesCount > 0) {
          List<String> indexNames = addDocumentRequest.getIndexNamesList();
          for (String currentIndexName : indexNames) {
            processIndexDocument(addDocumentRequest, currentIndexName);
          }
        } else {
          processIndexDocument(addDocumentRequest, indexName);
        }
      }

      @Override
      public void onError(Throwable t) {
        logger.warn("addDocuments Cancelled", t);
        responseObserver.onError(t);
      }

      private String onCompletedForIndex(String indexName) {
        ArrayBlockingQueue<AddDocumentRequest> addDocumentRequestQueue =
            getAddDocumentRequestQueue(indexName);
        logger.debug(
            String.format(
                "onCompleted, addDocumentRequestQueue: %s", addDocumentRequestQueue.size()));
        long highestGen = -1;
        try {
          DeadlineUtils.checkDeadline("addDocuments: onCompletedForIndex", "INDEXING");

          // index the left over docs
          if (!addDocumentRequestQueue.isEmpty()) {
            logger.debug(
                String.format(
                    "indexing left over addDocumentRequestQueue of size: %s",
                    addDocumentRequestQueue.size()));
            List<AddDocumentRequest> addDocRequestList = new ArrayList<>(addDocumentRequestQueue);
            // Since we are already running in the indexing threadpool run the indexing job
            // for remaining documents directly. This serializes indexing remaining documents for
            // multiple indices but avoids deadlocking if there aren't more threads than the
            // maximum
            // number of parallel addDocuments calls.
            long gen =
                new DocumentIndexer(getGlobalState(), addDocRequestList, indexName)
                    .runIndexingJob();
            if (gen > highestGen) {
              highestGen = gen;
            }
          }
          // collect futures, block if needed
          int numIndexingChunks = futures.size();
          long t0 = System.nanoTime();
          for (Future<Long> result : futures.get(indexName)) {
            Long gen = result.get();
            logger.debug("Indexing returned sequence-number {}", gen);
            if (gen > highestGen) {
              highestGen = gen;
            }
          }
          long t1 = System.nanoTime();
          logger.debug(
              "Indexing job completed for {} docs, in {} chunks, with latest sequence number: {}, took: {} micro seconds",
              getCount(indexName),
              numIndexingChunks,
              highestGen,
              ((t1 - t0) / 1000));
          return String.valueOf(highestGen);
        } catch (Exception e) {
          logger.warn("error while trying to addDocuments", e);
          throw Status.INTERNAL
              .withDescription("error while trying to addDocuments ")
              .augmentDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException();
        } finally {
          addDocumentRequestQueue.clear();
          countMap.put(indexName, 0L);
        }
      }

      @Override
      public void onCompleted() {
        try {
          getGlobalState()
              .submitIndexingTask(
                  Context.current()
                      .wrap(
                          () -> {
                            try {
                              // TODO: this should return a map on index to genId in the response
                              String genId = "-1";
                              for (String indexName : addDocumentRequestQueueMap.keySet()) {
                                genId = onCompletedForIndex(indexName);
                              }
                              responseObserver.onNext(
                                  AddDocumentResponse.newBuilder()
                                      .setGenId(genId)
                                      .setPrimaryId(getGlobalState().getEphemeralId())
                                      .build());
                              responseObserver.onCompleted();
                            } catch (Throwable t) {
                              responseObserver.onError(t);
                            }
                            return null;
                          }));
        } catch (RejectedExecutionException e) {
          logger.error("Threadpool is full, unable to submit indexing completion job");
          responseObserver.onError(
              Status.RESOURCE_EXHAUSTED
                  .withDescription("Threadpool is full, unable to submit indexing completion job")
                  .augmentDescription(e.getMessage())
                  .asRuntimeException());
        }
      }
    };
  }

  /**
   * DocumentsContext is created for each GRPC AddDocumentRequest It hold all lucene documents
   * context for the AddDocumentRequest including root document and optional child documents if
   * schema contains nested objects
   */
  public static class DocumentsContext {
    private final Document rootDocument;
    private final Map<String, List<Document>> childDocuments;

    public DocumentsContext() {
      this.rootDocument = new Document();
      this.childDocuments = new HashMap<>();
    }

    public Document getRootDocument() {
      return rootDocument;
    }

    public Map<String, List<Document>> getChildDocuments() {
      return childDocuments;
    }

    public void addChildDocuments(String key, List<Document> documents) {
      childDocuments.put(key, documents);
    }

    public boolean hasNested() {
      return !childDocuments.isEmpty();
    }
  }

  public static class LuceneDocumentBuilder {

    public static DocumentsContext getDocumentsContext(
        AddDocumentRequest addDocumentRequest, IndexState indexState)
        throws AddDocumentHandlerException {
      DocumentsContext documentsContext = new DocumentsContext();
      Map<String, AddDocumentRequest.MultiValuedField> fields = addDocumentRequest.getFieldsMap();
      for (Map.Entry<String, AddDocumentRequest.MultiValuedField> entry : fields.entrySet()) {
        parseOneField(entry.getKey(), entry.getValue(), documentsContext, indexState);
      }

      ((IndexableFieldDef<?>) (IndexState.getMetaField(IndexState.NESTED_PATH)))
          .parseDocumentField(
              documentsContext.getRootDocument(), List.of(IndexState.ROOT), List.of());

      // Include all fields and meta-fields in field names
      extractFieldNames(documentsContext);
      return documentsContext;
    }

    /** Extract all field names for each document and stores it into a hidden field */
    private static void extractFieldNames(DocumentsContext documentsContext) {
      extractFieldNamesForDocument(documentsContext.getRootDocument());
      documentsContext.getChildDocuments().values().stream()
          .flatMap(List::stream)
          .forEach(LuceneDocumentBuilder::extractFieldNamesForDocument);
    }

    /** Extract all field names in the document and stores it into a hidden field */
    private static void extractFieldNamesForDocument(Document document) {
      IndexableFieldDef<?> fieldNamesFieldDef =
          (IndexableFieldDef<?>) IndexState.getMetaField(IndexState.FIELD_NAMES);

      List<String> fieldNames =
          document.getFields().stream()
              .map(IndexableField::name)
              .distinct()
              .collect(Collectors.toList());

      fieldNamesFieldDef.parseDocumentField(document, fieldNames, List.of());
    }

    /** Parses a field's value, which is a MultiValuedField in all cases */
    private static void parseOneField(
        String fieldName,
        AddDocumentRequest.MultiValuedField value,
        DocumentsContext documentsContext,
        IndexState indexState)
        throws AddDocumentHandlerException {
      parseMultiValueField(indexState.getFieldOrThrow(fieldName), value, documentsContext);
    }

    /** Parse MultiValuedField for a single field, which is always a List<String>. */
    private static void parseMultiValueField(
        FieldDef field,
        AddDocumentRequest.MultiValuedField value,
        DocumentsContext documentsContext)
        throws AddDocumentHandlerException {
      ProtocolStringList fieldValues = value.getValueList();
      List<FacetHierarchyPath> facetHierarchyPaths = value.getFaceHierarchyPathsList();
      List<List<String>> facetHierarchyPathValues =
          facetHierarchyPaths.stream()
              .map(FacetHierarchyPath::getValueList)
              .collect(Collectors.toList());
      if (!facetHierarchyPathValues.isEmpty()) {
        if (facetHierarchyPathValues.size() != fieldValues.size()) {
          throw new AddDocumentHandlerException(
              String.format(
                  "Field: %s, fieldValues.size(): %s !=  "
                      + "facetHierarchyPaths.size(): %s, must have same list size for "
                      + "fieldValues and facetHierarchyPaths",
                  field.getName(), fieldValues.size(), facetHierarchyPathValues.size()));
        }
      }
      if (!(field instanceof IndexableFieldDef<?> indexableFieldDef)) {
        throw new AddDocumentHandlerException(
            String.format("Field: %s is not indexable", field.getName()));
      }
      indexableFieldDef.parseFieldWithChildren(
          documentsContext, fieldValues, facetHierarchyPathValues);
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

    public static final double ONE_MILLION = 1000000.0;
    private final GlobalState globalState;
    private final List<AddDocumentRequest> addDocumentRequestList;
    private final String indexName;

    public DocumentIndexer(
        GlobalState globalState,
        List<AddDocumentRequest> addDocumentRequestList,
        String indexName) {
      this.globalState = globalState;
      this.addDocumentRequestList = addDocumentRequestList;
      this.indexName = indexName;
    }

    public long runIndexingJob() throws Exception {
      DeadlineUtils.checkDeadline("DocumentIndexer: runIndexingJob", "INDEXING");

      logger.debug(
          String.format(
              "running indexing job on threadId: %s",
              Thread.currentThread().getName() + Thread.currentThread().threadId()));
      Queue<Document> documents = new LinkedBlockingDeque<>();
      IndexState indexState;
      ShardState shardState;
      IdFieldDef idFieldDef;

      try {
        indexState = globalState.getIndexOrThrow(this.indexName);
        shardState = indexState.getShard(0);
        idFieldDef = indexState.getIdFieldDef().orElse(null);
        for (AddDocumentRequest addDocumentRequest : addDocumentRequestList) {
          if (addDocumentRequest.getRequestType().equals(IndexingRequestType.UPDATE_DOC_VALUES)) {
            executeDocValueUpdateRequest(indexState, shardState, addDocumentRequest);
            continue;
          }
          DocumentsContext documentsContext =
              AddDocumentHandler.LuceneDocumentBuilder.getDocumentsContext(
                  addDocumentRequest, indexState);
          if (documentsContext.hasNested()) {
            try {
              if (idFieldDef != null) {
                // update documents in the queue to keep order
                updateDocuments(documents, idFieldDef, indexState, shardState);
                updateNestedDocuments(documentsContext, idFieldDef, indexState, shardState);
              } else {
                // add documents in the queue to keep order
                addDocuments(documents, indexState, shardState);
                addNestedDocuments(documentsContext, indexState, shardState);
              }
              documents.clear();
            } catch (IOException e) { // This exception should be caught in parent to and set
              // responseObserver.onError(e) so client knows the job failed
              logger.warn(
                  String.format(
                      "ThreadId: %s, IndexWriter.addDocuments failed",
                      Thread.currentThread().getName() + Thread.currentThread().threadId()));
              throw new IOException(e);
            }
          } else {
            documents.add(documentsContext.getRootDocument());
          }
        }
      } catch (Exception e) {
        logger.warn("addDocuments Cancelled", e);
        throw e; // parent thread should catch and send error back to client
      }

      try {
        if (idFieldDef != null) {
          updateDocuments(documents, idFieldDef, indexState, shardState);
        } else {
          addDocuments(documents, indexState, shardState);
        }
      } catch (IOException e) { // This exception should be caught in parent to and set
        // responseObserver.onError(e) so client knows the job failed
        logger.warn(
            String.format(
                "ThreadId: %s, IndexWriter.addDocuments failed",
                Thread.currentThread().getName() + Thread.currentThread().threadId()));
        throw new IOException(e);
      }
      logger.debug(
          String.format(
              "indexing job on threadId: %s done with SequenceId: %s",
              Thread.currentThread().getName() + Thread.currentThread().threadId(),
              shardState.writer.getMaxCompletedSequenceNumber()));
      return shardState.writer.getMaxCompletedSequenceNumber();
    }

    private void executeDocValueUpdateRequest(
        IndexState indexState, ShardState shardState, AddDocumentRequest addDocumentRequest) {
      try {
        IndexingMetrics.updateDocValuesRequestsReceived.labelValues(indexName).inc();
        Term term = buildTermForDocValueUpdate(indexState, addDocumentRequest);
        List<Field> updatableDocValueFields = new ArrayList<>();
        for (Map.Entry<String, MultiValuedField> entry :
            addDocumentRequest.getFieldsMap().entrySet()) {
          FieldDef field = indexState.getField(entry.getKey());
          if (field == null) {
            throw new IllegalArgumentException(
                String.format("Field: %s is not registered", entry.getKey()));
          }
          if (field.getName().equals(indexState.getIdFieldDef().get().getName())) continue;

          if (!(field instanceof DocValueUpdatable updatable) || !(updatable.isUpdatable())) {
            throw new IllegalArgumentException(
                String.format("Field: %s is not updatable", field.getName()));
          }
          if (entry.getValue().getValueCount() > 0) {
            updatableDocValueFields.add(
                ((DocValueUpdatable) field)
                    .getUpdatableDocValueField(entry.getValue().getValueList()));
          }
        }

        if (updatableDocValueFields.size() > 0) {
          long ns_start = System.nanoTime();
          shardState.writer.updateDocValues(term, updatableDocValueFields.toArray(new Field[0]));
          IndexingMetrics.updateDocValuesLatency
              .labelValues(indexName)
              .observe((System.nanoTime() - ns_start) / ONE_MILLION);
        }
      } catch (Throwable t) {
        logger.warn(
            String.format(
                "ThreadId: %s, IndexWriter.updateDocValues failed",
                Thread.currentThread().getName() + Thread.currentThread().threadId()));
        throw new RuntimeException("Error occurred when updating docValues ", t);
      }
    }

    private static Term buildTermForDocValueUpdate(
        IndexState indexState, AddDocumentRequest addDocumentRequest) {
      if (indexState.getIdFieldDef().isEmpty()) {
        throw new RuntimeException(
            " Index needs to have an ID field to execute update DocValue request");
      }
      String idFieldName = indexState.getIdFieldDef().get().getName();
      if (addDocumentRequest.getFieldsMap().get(idFieldName).getValueCount() == 0) {
        throw new IllegalArgumentException(
            String.format("the _ID should have a value set to execute update DocValue"));
      }
      String idFieldValue = addDocumentRequest.getFieldsMap().get(idFieldName).getValue(0);

      if (idFieldValue == null || idFieldValue.isEmpty()) {
        throw new IllegalArgumentException(String.format("the value of _ID field cannot be emtpy"));
      }
      return new Term(idFieldName, idFieldValue);
    }

    /**
     * update documents with nested objects
     *
     * @param documentsContext
     * @param idFieldDef
     * @param shardState
     * @throws IOException
     */
    private void updateNestedDocuments(
        DocumentsContext documentsContext,
        IdFieldDef idFieldDef,
        IndexState indexState,
        ShardState shardState)
        throws IOException {
      List<Document> documents = new ArrayList<>();
      for (Map.Entry<String, List<Document>> e : documentsContext.getChildDocuments().entrySet()) {
        documents.addAll(
            e.getValue().stream().map(v -> handleFacets(indexState, shardState, v)).toList());
      }

      addGlobalNestedDocumentOffsets(documents);

      Document rootDoc = handleFacets(indexState, shardState, documentsContext.getRootDocument());

      for (Document doc : documents) {
        for (IndexableField f : rootDoc.getFields(idFieldDef.getName())) {
          doc.add(f);
        }
      }

      documents.add(rootDoc);
      IndexingMetrics.addDocumentRequestsReceived.labelValues(indexName).inc();
      long ns_start = System.nanoTime();
      shardState.writer.updateDocuments(idFieldDef.getTerm(rootDoc), documents);
      IndexingMetrics.addDocumentLatency
          .labelValues(indexName)
          .observe((System.nanoTime() - ns_start) / ONE_MILLION);
    }

    /**
     * Add documents with nested object
     *
     * @param documentsContext
     * @param shardState
     * @throws IOException
     */
    private void addNestedDocuments(
        DocumentsContext documentsContext, IndexState indexState, ShardState shardState)
        throws IOException {
      List<Document> documents = new ArrayList<>();
      for (Map.Entry<String, List<Document>> e : documentsContext.getChildDocuments().entrySet()) {
        documents.addAll(
            e.getValue().stream().map(v -> handleFacets(indexState, shardState, v)).toList());
      }

      addGlobalNestedDocumentOffsets(documents);

      Document rootDoc = handleFacets(indexState, shardState, documentsContext.getRootDocument());
      documents.add(rootDoc);
      IndexingMetrics.addDocumentRequestsReceived.labelValues(indexName).inc();
      long ns_start = System.nanoTime();
      shardState.writer.addDocuments(documents);
      IndexingMetrics.addDocumentLatency
          .labelValues(indexName)
          .observe((System.nanoTime() - ns_start) / ONE_MILLION);
    }

    private void updateDocuments(
        Queue<Document> documents,
        IdFieldDef idFieldDef,
        IndexState indexState,
        ShardState shardState)
        throws IOException {
      if (shardState.isReplica()) {
        throw new IllegalStateException(
            "Adding documents to an index on a replica node is not supported");
      }
      for (Document nextDoc : documents) {
        nextDoc = handleFacets(indexState, shardState, nextDoc);
        IndexingMetrics.addDocumentRequestsReceived.labelValues(indexName).inc();
        long ns_start = System.nanoTime();
        shardState.writer.updateDocument(idFieldDef.getTerm(nextDoc), nextDoc);
        IndexingMetrics.addDocumentLatency
            .labelValues(indexName)
            .observe((System.nanoTime() - ns_start) / ONE_MILLION);
      }
    }

    private void addDocuments(
        Queue<Document> documents, IndexState indexState, ShardState shardState)
        throws IOException {
      if (shardState.isReplica()) {
        throw new IllegalStateException(
            "Adding documents to an index on a replica node is not supported");
      }
      if (documents != null)
        IndexingMetrics.addDocumentRequestsReceived.labelValues(indexName).inc(documents.size());
      long ns_start = System.nanoTime();
      shardState.writer.addDocuments(
          (Iterable<Document>)
              () ->
                  new Iterator<>() {
                    private Document nextDoc;

                    @Override
                    public boolean hasNext() {
                      if (!documents.isEmpty()) {
                        nextDoc = documents.poll();
                        nextDoc = handleFacets(indexState, shardState, nextDoc);
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
                  });
      if (documents != null && documents.size() >= 1) {
        IndexingMetrics.addDocumentLatency
            .labelValues(indexName)
            .observe((System.nanoTime() - ns_start) / ONE_MILLION / documents.size());
      }
    }

    private Document handleFacets(IndexState indexState, ShardState shardState, Document nextDoc) {
      final boolean hasFacets = indexState.hasFacets();
      if (hasFacets) {
        try {
          nextDoc = indexState.getFacetsConfig().build(shardState.taxoWriter, nextDoc);
        } catch (IOException ioe) {
          throw new RuntimeException(
              String.format("document: %s hit exception building facets", nextDoc), ioe);
        }
      }
      return nextDoc;
    }

    /**
     * Adds global offset values to nested documents for proper ordering and retrieval.
     *
     * <p>This method calculates and assigns a global offset to each nested document within a parent
     * document. The offset calculation uses reverse ordering (totalNestedDocs - currentIndex)
     *
     * @param nestedDocuments the list of nested documents to process; must not be null or empty
     * @throws IllegalArgumentException if nestedDocuments is null
     */
    private void addGlobalNestedDocumentOffsets(List<Document> nestedDocuments) {
      int totalNestedDocs = nestedDocuments.size();
      for (int i = 0; i < totalNestedDocs; i++) {
        int globalOffset = totalNestedDocs - i;
        nestedDocuments
            .get(i)
            .add(
                new org.apache.lucene.document.NumericDocValuesField(
                    IndexState.NESTED_DOCUMENT_OFFSET, globalOffset));
      }
    }

    @Override
    public Long call() throws Exception {
      return runIndexingJob();
    }
  }
}
