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

import com.google.protobuf.ProtocolStringList;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FacetHierarchyPath;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IdFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddDocumentHandler {
  private static final Logger logger = LoggerFactory.getLogger(AddDocumentHandler.class);

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

      ((IndexableFieldDef) (IndexState.getMetaField(IndexState.NESTED_PATH)))
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
      IndexableFieldDef fieldNamesFieldDef =
          (IndexableFieldDef) IndexState.getMetaField(IndexState.FIELD_NAMES);

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
      parseMultiValueField(indexState.getField(fieldName), value, documentsContext);
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
          facetHierarchyPaths.stream().map(fp -> fp.getValueList()).collect(Collectors.toList());
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
      if (!(field instanceof IndexableFieldDef)) {
        throw new AddDocumentHandlerException(
            String.format("Field: %s is not indexable", field.getName()));
      }
      IndexableFieldDef indexableFieldDef = (IndexableFieldDef) field;
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
      logger.debug(
          String.format(
              "running indexing job on threadId: %s",
              Thread.currentThread().getName() + Thread.currentThread().getId()));
      Queue<Document> documents = new LinkedBlockingDeque<>();
      IndexState indexState;
      ShardState shardState;
      IdFieldDef idFieldDef;

      try {
        indexState = globalState.getIndex(this.indexName);
        shardState = indexState.getShard(0);
        idFieldDef = indexState.getIdFieldDef().orElse(null);
        for (AddDocumentRequest addDocumentRequest : addDocumentRequestList) {
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
                      Thread.currentThread().getName() + Thread.currentThread().getId()));
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
                Thread.currentThread().getName() + Thread.currentThread().getId()));
        throw new IOException(e);
      }
      logger.debug(
          String.format(
              "indexing job on threadId: %s done with SequenceId: %s",
              Thread.currentThread().getName() + Thread.currentThread().getId(),
              shardState.writer.getMaxCompletedSequenceNumber()));
      return shardState.writer.getMaxCompletedSequenceNumber();
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
            e.getValue().stream()
                .map(v -> handleFacets(indexState, shardState, v))
                .collect(Collectors.toList()));
      }
      Document rootDoc = handleFacets(indexState, shardState, documentsContext.getRootDocument());

      for (Document doc : documents) {
        for (IndexableField f : rootDoc.getFields(idFieldDef.getName())) {
          doc.add(f);
        }
      }

      documents.add(rootDoc);
      shardState.writer.updateDocuments(idFieldDef.getTerm(rootDoc), documents);
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
            e.getValue().stream()
                .map(v -> handleFacets(indexState, shardState, v))
                .collect(Collectors.toList()));
      }
      Document rootDoc = handleFacets(indexState, shardState, documentsContext.getRootDocument());
      documents.add(rootDoc);
      shardState.writer.addDocuments(documents);
    }

    private void updateDocuments(
        Queue<Document> documents,
        IdFieldDef idFieldDef,
        IndexState indexState,
        ShardState shardState)
        throws IOException {
      for (Document nextDoc : documents) {
        nextDoc = handleFacets(indexState, shardState, nextDoc);
        shardState.writer.updateDocument(idFieldDef.getTerm(nextDoc), nextDoc);
      }
    }

    private void addDocuments(
        Queue<Document> documents, IndexState indexState, ShardState shardState)
        throws IOException {
      if (shardState.isReplica()) {
        throw new IllegalStateException(
            "Adding documents to an index on a replica node is not supported");
      }
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

    @Override
    public Long call() throws Exception {
      return runIndexingJob();
    }
  }
}
