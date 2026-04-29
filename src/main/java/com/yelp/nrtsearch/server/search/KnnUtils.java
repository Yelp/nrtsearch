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
package com.yelp.nrtsearch.server.search;

import com.yelp.nrtsearch.server.embedding.EmbeddingCreator;
import com.yelp.nrtsearch.server.embedding.EmbeddingProvider;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.VectorFieldDef;
import com.yelp.nrtsearch.server.field.properties.VectorQueryable;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.query.MinThresholdQuery;
import com.yelp.nrtsearch.server.query.QueryNodeMapper;
import com.yelp.nrtsearch.server.query.vector.WithVectorTotalHits;
import java.io.IOException;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;

public class KnnUtils {
  /**
   * Resolve (execute) the knn query and apply the boost. Resolving the query produces a new query
   * that matches the vector top hits. The boost is applied to this new query.
   *
   * @param knnQuery lucene knn query
   * @param boost boost to apply to the query
   * @param indexSearcher index searcher
   * @param vectorDiagnosticsBuilder diagnostics builder for vector search
   * @return vector search results query with boost applied
   * @throws IOException if query rewrite fails
   */
  public static Query resolveKnnQueryAndBoost(
      Query knnQuery,
      float boost,
      IndexSearcher indexSearcher,
      SearchResponse.Diagnostics.VectorDiagnostics.Builder vectorDiagnosticsBuilder)
      throws IOException {

    long vectorSearchStart = System.nanoTime();
    // Rewriting the query executes the vector search using the executor from the index searcher
    Query rewrittenQuery = knnQuery.rewrite(indexSearcher);

    // fill diagnostic info
    vectorDiagnosticsBuilder.setSearchTimeMs(((System.nanoTime() - vectorSearchStart) / 1000000.0));
    setVectorTotalHits(knnQuery, vectorDiagnosticsBuilder);

    if (boost != 1.0f) {
      rewrittenQuery = new BoostQuery(rewrittenQuery, boost);
    }
    return rewrittenQuery;
  }

  private static void setVectorTotalHits(
      Query knnQuery,
      SearchResponse.Diagnostics.VectorDiagnostics.Builder vectorDiagnosticsBuilder) {
    Query vectorQuery = knnQuery;
    if (vectorQuery instanceof MinThresholdQuery minThresholdQuery) {
      vectorQuery = minThresholdQuery.getWrapped();
    }
    if (vectorQuery instanceof WithVectorTotalHits withVectorTotalHits) {
      TotalHits vectorTotalHits = withVectorTotalHits.getTotalHits();
      vectorDiagnosticsBuilder.setTotalHits(
          com.yelp.nrtsearch.server.grpc.TotalHits.newBuilder()
              .setRelation(
                  com.yelp.nrtsearch.server.grpc.TotalHits.Relation.valueOf(
                      vectorTotalHits.relation().name()))
              .setValue(vectorTotalHits.value())
              .build());
    }
  }

  /**
   * Result of building and resolving a KNN query, holding the rewritten query and vector
   * diagnostics captured during rewrite.
   */
  public record KnnResolveResult(
      Query resolvedQuery, SearchResponse.Diagnostics.VectorDiagnostics vectorDiagnostics) {}

  /**
   * Build and resolve (execute) a KNN query, capturing vector diagnostics. No boost is applied —
   * the retriever boost is stored in RetrieverContext and applied by the blender. Intended to be
   * submitted to a dedicated executor so multiple KNN retrievers can run in parallel without using
   * the search executor.
   *
   * @param knnQuery knn query definition
   * @param indexState index state
   * @param indexSearcher index searcher
   * @return resolved query and vector diagnostics
   * @throws IOException if query rewrite fails
   */
  public static KnnResolveResult buildAndResolveKnnQuery(
      KnnQuery knnQuery, IndexState indexState, IndexSearcher indexSearcher) throws IOException {
    SearchResponse.Diagnostics.VectorDiagnostics.Builder vectorDiagnosticsBuilder =
        SearchResponse.Diagnostics.VectorDiagnostics.newBuilder();
    Query resolvedQuery =
        resolveKnnQueryAndBoost(
            buildKnnQuery(knnQuery, indexState), 1.0f, indexSearcher, vectorDiagnosticsBuilder);
    return new KnnResolveResult(resolvedQuery, vectorDiagnosticsBuilder.build());
  }

  /**
   * Resolve query_text to a query vector if set. Returns a new KnnQuery with query_vector populated
   * from the embedding provider.
   *
   * @param knnQuery knn query definition
   * @param fieldDef vector field definition
   * @return knn query with query_vector populated if query_text was set, otherwise the original
   */
  public static KnnQuery resolveQueryText(KnnQuery knnQuery, VectorFieldDef<?> fieldDef) {
    if (!knnQuery.getQueryText().isEmpty()) {
      // Validate mutual exclusivity
      if (knnQuery.getQueryVectorCount() > 0 || !knnQuery.getQueryByteVector().isEmpty()) {
        throw new IllegalArgumentException(
            "query_text is mutually exclusive with query_vector and query_byte_vector");
      }

      // Resolve provider: explicit override > field's configured provider
      String providerName =
          knnQuery.getEmbeddingProvider().isEmpty()
              ? fieldDef.getEmbeddingProviderName()
              : knnQuery.getEmbeddingProvider();

      if (providerName == null || providerName.isEmpty()) {
        throw new IllegalArgumentException(
            "query_text requires an embedding provider, but none configured on field '"
                + knnQuery.getField()
                + "' and no embedding_provider specified in query");
      }

      EmbeddingProvider provider = EmbeddingCreator.getInstance().getProvider(providerName);
      if (provider == null) {
        throw new IllegalArgumentException("Embedding provider not found: " + providerName);
      }

      float[] vector = provider.embed(knnQuery.getQueryText());
      KnnQuery.Builder builder = knnQuery.toBuilder().clearQueryText();
      for (float v : vector) {
        builder.addQueryVector(v);
      }
      return builder.build();
    }
    return knnQuery;
  }

  /**
   * Construct lucene knn query from grpc knn query.
   *
   * @param knnQuery knn query definition
   * @param indexState index state
   * @return lucene knn query
   */
  public static Query buildKnnQuery(KnnQuery knnQuery, IndexState indexState) {
    String field = knnQuery.getField();
    FieldDef fieldDef = indexState.getFieldOrThrow(field);
    if (!(fieldDef instanceof VectorQueryable vectorQueryable)) {
      throw new IllegalArgumentException("Field does not support vector search: " + field);
    }

    // Resolve query_text to query_vector if needed
    if (fieldDef instanceof VectorFieldDef<?> vectorFieldDef) {
      knnQuery = resolveQueryText(knnQuery, vectorFieldDef);
    }

    // Path to nested document containing this field
    String fieldNestedPath = IndexState.getFieldBaseNestedPath(field, indexState);
    // Path to parent document, this will be null if the field is in the root document
    String parentNestedPath = IndexState.getFieldBaseNestedPath(fieldNestedPath, indexState);

    Query filterQuery;
    if (knnQuery.hasFilter()) {
      filterQuery = QueryNodeMapper.getInstance().getQuery(knnQuery.getFilter(), indexState);
    } else {
      filterQuery = null;
    }

    BitSetProducer parentBitSetProducer = null;
    if (parentNestedPath != null) {
      Query parentQuery =
          QueryNodeMapper.getInstance().getNestedPathQuery(indexState, parentNestedPath);
      parentBitSetProducer = new QueryBitSetProducer(parentQuery);
      if (filterQuery != null) {
        // Filter query is applied to the parent document only
        filterQuery =
            QueryNodeMapper.getInstance()
                .applyQueryNestedPath(filterQuery, indexState, parentNestedPath);
        filterQuery = new ToChildBlockJoinQuery(filterQuery, parentBitSetProducer);
      }
    }
    return vectorQueryable.getKnnQuery(knnQuery, filterQuery, parentBitSetProducer);
  }
}
