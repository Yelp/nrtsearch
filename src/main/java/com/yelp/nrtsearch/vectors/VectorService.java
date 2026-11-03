/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.vectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentResponse;
import com.yelp.nrtsearch.server.grpc.CommitRequest;
import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.DeleteIndexRequest;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.LastHitInfo;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.RescoreVector;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.grpc.StartIndexV2Request;
import com.yelp.nrtsearch.server.grpc.StopIndexRequest;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.VectorIndexingOptions;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the S3 Vectors API on top of an nrtsearch primary.
 *
 * <p>Each vector index is an nrtsearch index with these fields:
 *
 * <ul>
 *   <li>{@code _key}: the vector key ({@code _ID} field), used for upserts and deletes
 *   <li>{@code _sort_key}: copy of the key as a sortable atom, used to page ListVectors
 *   <li>{@code _vector}: the vector, quantized HNSW with full precision rescoring
 *   <li>{@code _meta}: the full metadata as JSON, stored only
 *   <li>{@code m_<key>}: one typed, searchable field per filterable metadata key
 * </ul>
 *
 * Writes refresh the index before returning, so the next query sees them (strong consistency).
 */
public class VectorService {
  private static final Logger logger = LoggerFactory.getLogger(VectorService.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static final String KEY_FIELD = "_key";
  static final String SORT_KEY_FIELD = "_sort_key";
  static final String VECTOR_FIELD = "_vector";
  static final String META_FIELD = "_meta";

  public static final int MAX_VECTORS_PER_PUT = 500;
  public static final int MAX_VECTORS_PER_DELETE = 500;
  public static final int MAX_VECTORS_PER_GET = 100;
  public static final int MAX_TOP_K = 10_000;
  public static final int MAX_LIST_PAGE = 1000;
  public static final int MAX_METADATA_BYTES = 40 * 1024;
  public static final int MAX_FILTERABLE_METADATA_BYTES = 2 * 1024;
  public static final int MAX_METADATA_KEYS = 50;
  public static final int MAX_KEY_LENGTH = 1024;

  /** Settings for the gateway. */
  public static class Options {
    /** Quantization bits for the vector index, or 0 for no quantization (float32 HNSW). */
    public int quantizedBits = 7;

    /** How many extra candidates to rescore at full precision, when quantized. */
    public float rescoreOversample = 3.0f;

    /** Smallest num_candidates to use for HNSW search. */
    public int minNumCandidates = 100;

    /**
     * Write segments as separate files rather than compound (.cfs) files. Needed for replicas to
     * load the full precision vector files (.vec) lazily from S3, since files inside a compound
     * file can't be lazy.
     */
    public boolean disableCompoundFiles = true;

    /**
     * Stop an index after this long without requests, and start it again from S3 on the next
     * request. 0 disables tiering. Needs the nrtsearch server to use REMOTE data location.
     */
    public long idleStopMs = 0;

    /** How long a request waits for an index to start. */
    public long startTimeoutMs = 120_000;
  }

  private final LuceneServerGrpc.LuceneServerBlockingStub blockingStub;
  private final LuceneServerGrpc.LuceneServerStub asyncStub;
  private final BucketRegistry registry;
  private final Options options;
  private final IndexTiering tiering;

  public VectorService(
      LuceneServerGrpc.LuceneServerBlockingStub blockingStub,
      LuceneServerGrpc.LuceneServerStub asyncStub,
      BucketRegistry registry,
      Options options) {
    this.blockingStub = blockingStub;
    this.asyncStub = asyncStub;
    this.registry = registry;
    this.options = options;
    this.tiering = new IndexTiering(this::startNrtsearchIndex, this::stopNrtsearchIndex, options);
  }

  public IndexTiering getTiering() {
    return tiering;
  }

  public BucketRegistry getRegistry() {
    return registry;
  }

  // ---------------------------------------------------------------------------------------------
  // Buckets
  // ---------------------------------------------------------------------------------------------

  public ObjectNode createVectorBucket(JsonNode request) {
    registry.createBucket(text(request, "vectorBucketName", true));
    return obj();
  }

  public ObjectNode getVectorBucket(JsonNode request) {
    BucketRegistry.Bucket bucket = registry.getBucket(text(request, "vectorBucketName", true));
    ObjectNode response = obj();
    response.set("vectorBucket", bucketJson(bucket));
    return response;
  }

  public ObjectNode listVectorBuckets(JsonNode request) {
    String prefix = text(request, "prefix", false);
    ObjectNode response = obj();
    ArrayNode buckets = response.putArray("vectorBuckets");
    for (BucketRegistry.Bucket bucket : registry.listBuckets()) {
      if (prefix == null || bucket.name.startsWith(prefix)) {
        buckets.add(bucketJson(bucket));
      }
    }
    return response;
  }

  public ObjectNode deleteVectorBucket(JsonNode request) {
    registry.deleteBucket(text(request, "vectorBucketName", true));
    return obj();
  }

  private static ObjectNode bucketJson(BucketRegistry.Bucket bucket) {
    ObjectNode node = obj();
    node.put("vectorBucketName", bucket.name);
    node.put("creationTime", bucket.creationTime / 1000.0);
    return node;
  }

  // ---------------------------------------------------------------------------------------------
  // Indexes
  // ---------------------------------------------------------------------------------------------

  public ObjectNode createIndex(JsonNode request) {
    String bucketName = text(request, "vectorBucketName", true);
    String indexName = text(request, "indexName", true);
    String dataType = text(request, "dataType", false);
    if (dataType != null && !dataType.equals("float32")) {
      throw VectorsException.validation("dataType must be float32");
    }
    JsonNode dimensionNode = request.get("dimension");
    if (dimensionNode == null || !dimensionNode.isInt()) {
      throw VectorsException.validation("dimension is required and must be an integer");
    }
    List<String> nonFilterable = new ArrayList<>();
    JsonNode metadataConfig = request.get("metadataConfiguration");
    if (metadataConfig != null && metadataConfig.has("nonFilterableMetadataKeys")) {
      for (JsonNode key : metadataConfig.get("nonFilterableMetadataKeys")) {
        nonFilterable.add(key.asText());
      }
    }
    IndexConfig config =
        new IndexConfig(
            bucketName,
            indexName,
            dimensionNode.asInt(),
            IndexConfig.DistanceMetric.parse(text(request, "distanceMetric", true)),
            nonFilterable,
            System.currentTimeMillis());
    registry.getBucket(bucketName);

    String nrtIndex = config.nrtsearchIndexName();
    try {
      blockingStub.createIndex(
          CreateIndexRequest.newBuilder()
              .setIndexName(nrtIndex)
              .addAllFields(baseFields(config))
              .setLiveSettings(liveSettings())
              .build());
      blockingStub.startIndexV2(StartIndexV2Request.newBuilder().setIndexName(nrtIndex).build());
    } catch (StatusRuntimeException e) {
      throw fromGrpc("Could not create index " + indexName, e);
    }
    registry.addIndex(config);
    tiering.markStarted(nrtIndex);
    return obj();
  }

  public ObjectNode getIndex(JsonNode request) {
    IndexConfig config = indexConfig(request);
    ObjectNode response = obj();
    response.set("index", indexJson(config));
    return response;
  }

  public ObjectNode listIndexes(JsonNode request) {
    String bucketName = text(request, "vectorBucketName", true);
    String prefix = text(request, "prefix", false);
    ObjectNode response = obj();
    ArrayNode indexes = response.putArray("indexes");
    for (IndexConfig config : registry.listIndexes(bucketName)) {
      if (prefix == null || config.getIndexName().startsWith(prefix)) {
        indexes.add(indexJson(config));
      }
    }
    return response;
  }

  public ObjectNode deleteIndex(JsonNode request) {
    IndexConfig config = indexConfig(request);
    String nrtIndex = config.nrtsearchIndexName();
    try {
      blockingStub.deleteIndex(DeleteIndexRequest.newBuilder().setIndexName(nrtIndex).build());
    } catch (StatusRuntimeException e) {
      throw fromGrpc("Could not delete index " + config.getIndexName(), e);
    }
    tiering.forget(nrtIndex);
    registry.removeIndex(config.getBucketName(), config.getIndexName());
    return obj();
  }

  private static ObjectNode indexJson(IndexConfig config) {
    ObjectNode node = obj();
    node.put("vectorBucketName", config.getBucketName());
    node.put("indexName", config.getIndexName());
    node.put("dataType", "float32");
    node.put("dimension", config.getDimension());
    node.put("distanceMetric", config.getDistanceMetric().apiName());
    node.put("creationTime", config.getCreationTime() / 1000.0);
    ArrayNode keys = node.putObject("metadataConfiguration").putArray("nonFilterableMetadataKeys");
    config.getNonFilterableMetadataKeys().forEach(keys::add);
    return node;
  }

  private List<Field> baseFields(IndexConfig config) {
    VectorIndexingOptions.Builder vectorOptions = VectorIndexingOptions.newBuilder();
    int bits = quantizedBits(config);
    if (bits > 0) {
      vectorOptions.setType("hnsw_scalar_quantized").setQuantizedBits(bits);
    } else {
      vectorOptions.setType("hnsw");
    }
    return List.of(
        Field.newBuilder()
            .setName(KEY_FIELD)
            .setType(FieldType._ID)
            .setStoreDocValues(true)
            .build(),
        Field.newBuilder()
            .setName(SORT_KEY_FIELD)
            .setType(FieldType.ATOM)
            .setStoreDocValues(true)
            .build(),
        Field.newBuilder()
            .setName(VECTOR_FIELD)
            .setType(FieldType.VECTOR)
            .setSearch(true)
            .setStoreDocValues(true)
            .setVectorDimensions(config.getDimension())
            .setVectorSimilarity(config.getDistanceMetric().getNrtsearchSimilarity())
            .setVectorIndexingOptions(vectorOptions)
            .build(),
        Field.newBuilder().setName(META_FIELD).setType(FieldType.ATOM).setStore(true).build());
  }

  private IndexLiveSettings liveSettings() {
    IndexLiveSettings.Builder settings = IndexLiveSettings.newBuilder();
    if (options.disableCompoundFiles) {
      settings.setUseCompoundFile(BoolValue.of(false)).setNoCFSRatio(DoubleValue.of(0.0));
    }
    return settings.build();
  }

  private int quantizedBits(IndexConfig config) {
    int bits = options.quantizedBits;
    // 4 bit quantization needs an even number of dimensions
    if (bits == 4 && config.getDimension() % 2 != 0) {
      return 7;
    }
    return bits;
  }

  // ---------------------------------------------------------------------------------------------
  // Vectors
  // ---------------------------------------------------------------------------------------------

  public ObjectNode putVectors(JsonNode request) {
    IndexConfig config = indexConfig(request);
    JsonNode vectors = request.get("vectors");
    if (vectors == null || !vectors.isArray() || vectors.isEmpty()) {
      throw VectorsException.validation("vectors must be a non-empty array");
    }
    if (vectors.size() > MAX_VECTORS_PER_PUT) {
      throw VectorsException.validation(
          "At most " + MAX_VECTORS_PER_PUT + " vectors can be put in one request");
    }

    // Validate everything and collect new metadata keys before writing anything
    MetadataSchema schema = config.getMetadataSchema();
    Map<String, MetadataSchema.MetadataType> newKeys = new LinkedHashMap<>();
    java.util.Set<String> keysInRequest = new java.util.HashSet<>();
    for (JsonNode vector : vectors) {
      String key = validateKey(vector.get("key"));
      if (!keysInRequest.add(key)) {
        throw VectorsException.validation("Duplicate key in request: " + key);
      }
      validateVectorData(vector.get("data"), config);
      JsonNode metadata = vector.get("metadata");
      validateMetadata(metadata, config);
      for (Map.Entry<String, MetadataSchema.MetadataType> entry :
          schema.inferNewKeys(metadata, config).entrySet()) {
        MetadataSchema.MetadataType existing =
            newKeys.putIfAbsent(entry.getKey(), entry.getValue());
        if (existing != null && existing != entry.getValue()) {
          throw VectorsException.validation(
              "Metadata key '" + entry.getKey() + "' has more than one type in this request");
        }
      }
    }

    String nrtIndex = config.nrtsearchIndexName();
    tiering.withIndex(
        nrtIndex,
        () -> {
          registerMetadataFields(config, newKeys);
          List<AddDocumentRequest> docs = new ArrayList<>(vectors.size());
          for (JsonNode vector : vectors) {
            docs.add(toDocument(nrtIndex, vector, config));
          }
          addDocuments(docs);
          refresh(nrtIndex);
          return null;
        });
    return obj();
  }

  private void registerMetadataFields(
      IndexConfig config, Map<String, MetadataSchema.MetadataType> newKeys) {
    if (newKeys.isEmpty()) {
      return;
    }
    MetadataSchema schema = config.getMetadataSchema();
    synchronized (schema) {
      List<Field> fields = schema.addKeys(newKeys);
      if (fields.isEmpty()) {
        return;
      }
      try {
        blockingStub.updateFields(
            FieldDefRequest.newBuilder()
                .setIndexName(config.nrtsearchIndexName())
                .addAllField(fields)
                .build());
      } catch (StatusRuntimeException e) {
        schema.removeKeys(newKeys.keySet());
        throw fromGrpc("Could not register metadata fields", e);
      }
      registry.save();
    }
  }

  private AddDocumentRequest toDocument(String nrtIndex, JsonNode vector, IndexConfig config) {
    String key = vector.get("key").asText();
    AddDocumentRequest.Builder doc = AddDocumentRequest.newBuilder().setIndexName(nrtIndex);
    doc.putFields(KEY_FIELD, values(key));
    doc.putFields(SORT_KEY_FIELD, values(key));
    doc.putFields(VECTOR_FIELD, values(vector.get("data").get("float32").toString()));
    JsonNode metadata = vector.get("metadata");
    if (metadata != null && !metadata.isNull() && !metadata.isEmpty()) {
      doc.putFields(META_FIELD, values(metadata.toString()));
      MetadataSchema schema = config.getMetadataSchema();
      Iterator<Map.Entry<String, JsonNode>> it = metadata.fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> entry = it.next();
        if (!config.isFilterable(entry.getKey())) {
          continue;
        }
        String field = schema.getFieldName(entry.getKey());
        List<String> fieldValues = MetadataSchema.toFieldValues(entry.getValue());
        if (field != null && !fieldValues.isEmpty()) {
          doc.putFields(
              field,
              AddDocumentRequest.MultiValuedField.newBuilder().addAllValue(fieldValues).build());
        }
      }
    }
    return doc.build();
  }

  public ObjectNode getVectors(JsonNode request) {
    IndexConfig config = indexConfig(request);
    List<String> keys = stringList(request, "keys");
    if (keys.isEmpty()) {
      throw VectorsException.validation("keys must be a non-empty array");
    }
    if (keys.size() > MAX_VECTORS_PER_GET) {
      throw VectorsException.validation(
          "At most " + MAX_VECTORS_PER_GET + " vectors can be fetched in one request");
    }
    boolean returnData = bool(request, "returnData");
    boolean returnMetadata = bool(request, "returnMetadata");

    Query query =
        Query.newBuilder()
            .setTermInSetQuery(
                TermInSetQuery.newBuilder()
                    .setField(KEY_FIELD)
                    .setTextTerms(TermInSetQuery.TextTerms.newBuilder().addAllTerms(keys)))
            .build();
    SearchRequest.Builder search =
        SearchRequest.newBuilder()
            .setIndexName(config.nrtsearchIndexName())
            .setQuery(query)
            .setTopHits(keys.size())
            .addAllRetrieveFields(retrieveFields(returnData, returnMetadata));
    SearchResponse response = search(config, search.build());

    ObjectNode result = obj();
    ArrayNode out = result.putArray("vectors");
    for (SearchResponse.Hit hit : response.getHitsList()) {
      out.add(hitToVector(hit, returnData, returnMetadata, null, config));
    }
    return result;
  }

  public ObjectNode deleteVectors(JsonNode request) {
    IndexConfig config = indexConfig(request);
    List<String> keys = stringList(request, "keys");
    if (keys.isEmpty()) {
      throw VectorsException.validation("keys must be a non-empty array");
    }
    if (keys.size() > MAX_VECTORS_PER_DELETE) {
      throw VectorsException.validation(
          "At most " + MAX_VECTORS_PER_DELETE + " vectors can be deleted in one request");
    }
    String nrtIndex = config.nrtsearchIndexName();
    tiering.withIndex(
        nrtIndex,
        () -> {
          try {
            blockingStub.delete(
                AddDocumentRequest.newBuilder()
                    .setIndexName(nrtIndex)
                    .putFields(
                        KEY_FIELD,
                        AddDocumentRequest.MultiValuedField.newBuilder().addAllValue(keys).build())
                    .build());
          } catch (StatusRuntimeException e) {
            throw fromGrpc("Could not delete vectors", e);
          }
          refresh(nrtIndex);
          return null;
        });
    return obj();
  }

  public ObjectNode listVectors(JsonNode request) {
    IndexConfig config = indexConfig(request);
    int maxResults = request.has("maxResults") ? request.get("maxResults").asInt() : MAX_LIST_PAGE;
    if (maxResults < 1 || maxResults > MAX_LIST_PAGE) {
      throw VectorsException.validation("maxResults must be between 1 and " + MAX_LIST_PAGE);
    }
    boolean returnData = bool(request, "returnData");
    boolean returnMetadata = bool(request, "returnMetadata");

    SearchRequest.Builder search =
        SearchRequest.newBuilder()
            .setIndexName(config.nrtsearchIndexName())
            .setTopHits(maxResults)
            .setQuerySort(
                QuerySortField.newBuilder()
                    .setFields(
                        SortFields.newBuilder()
                            .addSortedFields(SortType.newBuilder().setFieldName(SORT_KEY_FIELD))))
            .addAllRetrieveFields(retrieveFields(returnData, returnMetadata));
    String nextToken = text(request, "nextToken", false);
    if (nextToken != null) {
      search.setSearchAfter(decodeToken(nextToken));
    }
    SearchResponse response = search(config, search.build());

    ObjectNode result = obj();
    ArrayNode out = result.putArray("vectors");
    for (SearchResponse.Hit hit : response.getHitsList()) {
      out.add(hitToVector(hit, returnData, returnMetadata, null, config));
    }
    if (response.getHitsCount() == maxResults && response.hasSearchState()) {
      result.put("nextToken", encodeToken(response.getSearchState().getLastHitInfo()));
    }
    return result;
  }

  public ObjectNode queryVectors(JsonNode request) {
    IndexConfig config = indexConfig(request);
    JsonNode topKNode = request.get("topK");
    if (topKNode == null || !topKNode.isInt()) {
      throw VectorsException.validation("topK is required and must be an integer");
    }
    int topK = topKNode.asInt();
    if (topK < 1 || topK > MAX_TOP_K) {
      throw VectorsException.validation("topK must be between 1 and " + MAX_TOP_K);
    }
    float[] queryVector = validateVectorData(request.get("queryVector"), config);
    boolean returnDistance = bool(request, "returnDistance");
    boolean returnMetadata = bool(request, "returnMetadata");

    KnnQuery.Builder knn =
        KnnQuery.newBuilder()
            .setField(VECTOR_FIELD)
            .setK(topK)
            .setNumCandidates(Math.min(MAX_TOP_K, Math.max(topK, options.minNumCandidates)))
            .setFilterStrategy(KnnQuery.FilterStrategy.ACORN);
    for (float v : queryVector) {
      knn.addQueryVector(v);
    }
    if (quantizedBits(config) > 0 && options.rescoreOversample >= 1.0f) {
      knn.setRescoreVector(RescoreVector.newBuilder().setOversample(options.rescoreOversample));
    }
    JsonNode filter = request.get("filter");
    if (filter != null && !filter.isNull()) {
      knn.setFilter(FilterTranslator.translate(filter, config));
    }

    SearchRequest search =
        SearchRequest.newBuilder()
            .setIndexName(config.nrtsearchIndexName())
            .addKnn(knn)
            .setTopHits(topK)
            .addAllRetrieveFields(retrieveFields(false, returnMetadata))
            .build();
    SearchResponse response = search(config, search);

    ObjectNode result = obj();
    ArrayNode out = result.putArray("vectors");
    for (SearchResponse.Hit hit : response.getHitsList()) {
      Float distance =
          returnDistance
              ? DistanceConverter.scoreToDistance(hit.getScore(), config.getDistanceMetric())
              : null;
      out.add(hitToVector(hit, false, returnMetadata, distance, config));
    }
    result.put("distanceMetric", config.getDistanceMetric().apiName());
    return result;
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private SearchResponse search(IndexConfig config, SearchRequest request) {
    return tiering.withIndex(
        config.nrtsearchIndexName(),
        () -> {
          try {
            return blockingStub.search(request);
          } catch (StatusRuntimeException e) {
            throw fromGrpc("Search failed", e);
          }
        });
  }

  private void addDocuments(List<AddDocumentRequest> docs) {
    CompletableFuture<AddDocumentResponse> future = new CompletableFuture<>();
    StreamObserver<AddDocumentRequest> requestObserver =
        asyncStub.addDocuments(
            new StreamObserver<>() {
              @Override
              public void onNext(AddDocumentResponse value) {
                future.complete(value);
              }

              @Override
              public void onError(Throwable t) {
                future.completeExceptionally(t);
              }

              @Override
              public void onCompleted() {
                future.complete(null);
              }
            });
    try {
      docs.forEach(requestObserver::onNext);
      requestObserver.onCompleted();
    } catch (RuntimeException e) {
      requestObserver.onError(e);
      throw e;
    }
    try {
      future.get(60, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof StatusRuntimeException sre) {
        throw fromGrpc("Could not add vectors", sre);
      }
      throw new RuntimeException("Could not add vectors", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new VectorsException("ServiceUnavailableException", 503, "Timed out adding vectors");
    }
  }

  private void refresh(String nrtIndex) {
    try {
      blockingStub.refresh(RefreshRequest.newBuilder().setIndexName(nrtIndex).build());
    } catch (StatusRuntimeException e) {
      throw fromGrpc("Could not refresh index", e);
    }
  }

  private void startNrtsearchIndex(String nrtIndex) {
    logger.info("Starting idle index {}", nrtIndex);
    blockingStub.startIndexV2(StartIndexV2Request.newBuilder().setIndexName(nrtIndex).build());
  }

  private void stopNrtsearchIndex(String nrtIndex) {
    logger.info("Stopping idle index {}", nrtIndex);
    // commit so that all data is backed up before the local copy goes away
    blockingStub.commit(CommitRequest.newBuilder().setIndexName(nrtIndex).build());
    blockingStub.stopIndex(StopIndexRequest.newBuilder().setIndexName(nrtIndex).build());
  }

  private static List<String> retrieveFields(boolean returnData, boolean returnMetadata) {
    List<String> fields = new ArrayList<>();
    fields.add(KEY_FIELD);
    if (returnData) {
      fields.add(VECTOR_FIELD);
    }
    if (returnMetadata) {
      fields.add(META_FIELD);
    }
    return fields;
  }

  private static ObjectNode hitToVector(
      SearchResponse.Hit hit,
      boolean returnData,
      boolean returnMetadata,
      Float distance,
      IndexConfig config) {
    ObjectNode node = obj();
    node.put("key", firstText(hit, KEY_FIELD));
    if (returnData) {
      SearchResponse.Hit.CompositeFieldValue value = hit.getFieldsMap().get(VECTOR_FIELD);
      ArrayNode data = node.putObject("data").putArray("float32");
      if (value != null && value.getFieldValueCount() > 0) {
        value.getFieldValue(0).getVectorValue().getValueList().forEach(data::add);
      }
    }
    if (distance != null) {
      node.put("distance", distance);
    }
    if (returnMetadata) {
      String meta = firstText(hit, META_FIELD);
      if (meta != null) {
        try {
          node.set("metadata", MAPPER.readTree(meta));
        } catch (Exception e) {
          throw new IllegalStateException("Stored metadata is not valid JSON", e);
        }
      } else {
        node.putObject("metadata");
      }
    }
    return node;
  }

  private static String firstText(SearchResponse.Hit hit, String field) {
    SearchResponse.Hit.CompositeFieldValue value = hit.getFieldsMap().get(field);
    if (value == null || value.getFieldValueCount() == 0) {
      return null;
    }
    return value.getFieldValue(0).getTextValue();
  }

  private IndexConfig indexConfig(JsonNode request) {
    String bucketName = text(request, "vectorBucketName", true);
    String indexName = text(request, "indexName", true);
    return registry.getIndex(bucketName, indexName);
  }

  private static String validateKey(JsonNode keyNode) {
    if (keyNode == null || !keyNode.isTextual() || keyNode.asText().isEmpty()) {
      throw VectorsException.validation("Each vector needs a non-empty string key");
    }
    String key = keyNode.asText();
    if (key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_LENGTH) {
      throw VectorsException.validation("Vector keys must be at most " + MAX_KEY_LENGTH + " bytes");
    }
    return key;
  }

  private static float[] validateVectorData(JsonNode data, IndexConfig config) {
    if (data == null || !data.has("float32") || !data.get("float32").isArray()) {
      throw VectorsException.validation("Vector data must be {\"float32\": [...]}");
    }
    JsonNode values = data.get("float32");
    if (values.size() != config.getDimension()) {
      throw VectorsException.validation(
          "Vector has "
              + values.size()
              + " dimensions, the index expects "
              + config.getDimension());
    }
    float[] vector = new float[values.size()];
    boolean allZero = true;
    for (int i = 0; i < vector.length; i++) {
      JsonNode v = values.get(i);
      if (!v.isNumber()) {
        throw VectorsException.validation("Vector values must be numbers");
      }
      vector[i] = (float) v.asDouble();
      if (!Float.isFinite(vector[i])) {
        throw VectorsException.validation("Vector values must be finite float32 numbers");
      }
      allZero &= vector[i] == 0f;
    }
    if (allZero && config.getDistanceMetric() == IndexConfig.DistanceMetric.COSINE) {
      throw VectorsException.validation("Zero vectors are not allowed with cosine distance");
    }
    return vector;
  }

  private static void validateMetadata(JsonNode metadata, IndexConfig config) {
    if (metadata == null || metadata.isNull()) {
      return;
    }
    if (!metadata.isObject()) {
      throw VectorsException.validation("metadata must be a JSON object");
    }
    if (metadata.size() > MAX_METADATA_KEYS) {
      throw VectorsException.validation(
          "A vector can have at most " + MAX_METADATA_KEYS + " metadata keys");
    }
    int total = metadata.toString().getBytes(StandardCharsets.UTF_8).length;
    if (total > MAX_METADATA_BYTES) {
      throw VectorsException.validation(
          "Metadata is " + total + " bytes, the maximum is " + MAX_METADATA_BYTES);
    }
    ObjectNode filterable = obj();
    Iterator<Map.Entry<String, JsonNode>> it = metadata.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      if (config.isFilterable(entry.getKey())) {
        filterable.set(entry.getKey(), entry.getValue());
      }
    }
    int filterableBytes = filterable.toString().getBytes(StandardCharsets.UTF_8).length;
    if (filterableBytes > MAX_FILTERABLE_METADATA_BYTES) {
      throw VectorsException.validation(
          "Filterable metadata is "
              + filterableBytes
              + " bytes, the maximum is "
              + MAX_FILTERABLE_METADATA_BYTES);
    }
  }

  static String encodeToken(LastHitInfo lastHitInfo) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(lastHitInfo.toByteArray());
  }

  static LastHitInfo decodeToken(String token) {
    try {
      return LastHitInfo.parseFrom(Base64.getUrlDecoder().decode(token));
    } catch (Exception e) {
      throw VectorsException.validation("Invalid nextToken");
    }
  }

  private static AddDocumentRequest.MultiValuedField values(String value) {
    return AddDocumentRequest.MultiValuedField.newBuilder().addValue(value).build();
  }

  private static ObjectNode obj() {
    return JsonNodeFactory.instance.objectNode();
  }

  private static String text(JsonNode request, String field, boolean required) {
    JsonNode node = request == null ? null : request.get(field);
    if (node == null || node.isNull()) {
      if (required) {
        throw VectorsException.validation(field + " is required");
      }
      return null;
    }
    if (!node.isTextual()) {
      throw VectorsException.validation(field + " must be a string");
    }
    return node.asText();
  }

  private static boolean bool(JsonNode request, String field) {
    JsonNode node = request.get(field);
    return node != null && node.asBoolean(false);
  }

  private static List<String> stringList(JsonNode request, String field) {
    JsonNode node = request.get(field);
    List<String> values = new ArrayList<>();
    if (node == null || !node.isArray()) {
      return values;
    }
    for (JsonNode element : node) {
      values.add(validateKey(element));
    }
    return values;
  }

  static VectorsException fromGrpc(String message, StatusRuntimeException e) {
    Status.Code code = e.getStatus().getCode();
    String detail = message + ": " + e.getStatus().getDescription();
    return switch (code) {
      case INVALID_ARGUMENT -> VectorsException.validation(detail);
      case NOT_FOUND -> VectorsException.notFound(detail);
      case ALREADY_EXISTS -> VectorsException.conflict(detail);
      case UNAVAILABLE, DEADLINE_EXCEEDED ->
          new VectorsException("ServiceUnavailableException", 503, detail);
      default -> new VectorsException("InternalServerException", 500, detail);
    };
  }
}
