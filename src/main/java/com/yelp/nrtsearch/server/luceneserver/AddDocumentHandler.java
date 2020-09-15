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

import com.google.protobuf.Any;
import com.google.protobuf.ProtocolStringList;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FacetHierarchyPath;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IdFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddDocumentHandler implements Handler<AddDocumentRequest, Any> {
  private static final Logger logger = LoggerFactory.getLogger(AddDocumentHandler.class);

  @Override
  public Any handle(IndexState indexState, AddDocumentRequest addDocumentRequest)
      throws AddDocumentHandlerException {
    Document document = LuceneDocumentBuilder.getDocument(addDocumentRequest, indexState);
    // this is silly we dont really about about the return value here
    return Any.newBuilder().build();
  }

  public static class LuceneDocumentBuilder {
    public static Document getDocument(AddDocumentRequest addDocumentRequest, IndexState indexState)
        throws AddDocumentHandlerException {
      Document document = new Document();
      Map<String, AddDocumentRequest.MultiValuedField> fields = addDocumentRequest.getFieldsMap();
      for (Map.Entry<String, AddDocumentRequest.MultiValuedField> entry : fields.entrySet()) {
        parseOneField(entry.getKey(), entry.getValue(), document, indexState);
      }
      return document;
    }

    /** Parses a field's value, which is a MultiValuedField in all cases */
    private static void parseOneField(
        String key,
        AddDocumentRequest.MultiValuedField value,
        Document document,
        IndexState indexState)
        throws AddDocumentHandlerException {
      parseMultiValueField(indexState.getField(key), value, document);
    }

    /** Parse MultiValuedField for a single field, which is always a List<String>. */
    private static void parseMultiValueField(
        FieldDef field, AddDocumentRequest.MultiValuedField value, Document document)
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
      indexableFieldDef.parseDocumentField(document, fieldValues, facetHierarchyPathValues);
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

    public DocumentIndexer(
        GlobalState globalState, List<AddDocumentRequest> addDocumentRequestList) {
      this.globalState = globalState;
      this.addDocumentRequestList = addDocumentRequestList;
    }

    public long runIndexingJob() throws Exception {
      logger.debug(
          String.format(
              "running indexing job on threadId: %s",
              Thread.currentThread().getName() + Thread.currentThread().getId()));
      Queue<Document> documents = new LinkedBlockingDeque<>();
      IndexState indexState = null;
      for (AddDocumentRequest addDocumentRequest : addDocumentRequestList) {
        try {
          indexState = globalState.getIndex(addDocumentRequest.getIndexName());
          Document document =
              AddDocumentHandler.LuceneDocumentBuilder.getDocument(addDocumentRequest, indexState);
          documents.add(document);
        } catch (Exception e) {
          logger.warn("addDocuments Cancelled", e);
          throw new Exception(e); // parent thread should catch and send error back to client
        }
      }
      ShardState shardState = indexState.getShard(0);
      IdFieldDef idFieldDef = indexState.getIdFieldDef();
      try {
        if (idFieldDef != null) {
          updateDocuments(documents, idFieldDef, shardState);
        } else {
          addDocuments(documents, shardState);
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

    private void updateDocuments(
        Queue<Document> documents, IdFieldDef idFieldDef, ShardState shardState)
        throws IOException {
      for (Document nextDoc : documents) {
        nextDoc = handleFacets(shardState, nextDoc);
        shardState.writer.updateDocument(idFieldDef.getTerm(nextDoc), nextDoc);
      }
    }

    private void addDocuments(Queue<Document> documents, ShardState shardState) throws IOException {
      shardState.writer.addDocuments(
          (Iterable<Document>)
              () ->
                  new Iterator<>() {
                    private Document nextDoc;

                    @Override
                    public boolean hasNext() {
                      if (!documents.isEmpty()) {
                        nextDoc = documents.poll();
                        nextDoc = handleFacets(shardState, nextDoc);
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

    private Document handleFacets(ShardState shardState, Document nextDoc) {
      final boolean hasFacets = shardState.indexState.hasFacets();
      if (hasFacets) {
        try {
          nextDoc = shardState.indexState.facetsConfig.build(shardState.taxoWriter, nextDoc);
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
