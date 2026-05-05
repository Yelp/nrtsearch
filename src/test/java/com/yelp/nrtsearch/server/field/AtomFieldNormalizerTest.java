/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.PrefixQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery.TextTerms;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;

public class AtomFieldNormalizerTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsAtomNormalizer.json");
  }

  @Override
  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();

    // doc 1: mixed-case values
    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("atom_lower", MultiValuedField.newBuilder().addValue("Hello World").build())
            .putFields(
                "atom_lower_stored", MultiValuedField.newBuilder().addValue("Hello World").build())
            .putFields(
                "atom_lower_multi",
                MultiValuedField.newBuilder().addValue("Foo").addValue("BAR").build())
            .putFields(
                "atom_custom_lower", MultiValuedField.newBuilder().addValue("CUSTOM").build())
            .putFields(
                "atom_no_normalizer", MultiValuedField.newBuilder().addValue("Hello World").build())
            .putFields("atom_lower_prefix", MultiValuedField.newBuilder().addValue("Apple").build())
            .putFields("atom_lower_range", MultiValuedField.newBuilder().addValue("B").build())
            .build());

    // doc 2: already lowercase
    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields("atom_lower", MultiValuedField.newBuilder().addValue("hello world").build())
            .putFields(
                "atom_lower_stored", MultiValuedField.newBuilder().addValue("hello world").build())
            .putFields(
                "atom_lower_multi",
                MultiValuedField.newBuilder().addValue("foo").addValue("bar").build())
            .putFields(
                "atom_custom_lower", MultiValuedField.newBuilder().addValue("custom").build())
            .putFields(
                "atom_no_normalizer", MultiValuedField.newBuilder().addValue("hello world").build())
            .putFields(
                "atom_lower_prefix", MultiValuedField.newBuilder().addValue("apricot").build())
            .putFields("atom_lower_range", MultiValuedField.newBuilder().addValue("D").build())
            .build());

    // doc 3: uppercase
    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("3").build())
            .putFields("atom_lower", MultiValuedField.newBuilder().addValue("HELLO WORLD").build())
            .putFields(
                "atom_lower_stored", MultiValuedField.newBuilder().addValue("HELLO WORLD").build())
            .putFields(
                "atom_lower_multi",
                MultiValuedField.newBuilder().addValue("FOO").addValue("BAZ").build())
            .putFields(
                "atom_custom_lower", MultiValuedField.newBuilder().addValue("CUSTOM").build())
            .putFields(
                "atom_no_normalizer", MultiValuedField.newBuilder().addValue("HELLO WORLD").build())
            .putFields(
                "atom_lower_prefix", MultiValuedField.newBuilder().addValue("AVOCADO").build())
            .putFields("atom_lower_range", MultiValuedField.newBuilder().addValue("F").build())
            .build());

    addDocuments(docs.stream());
  }

  // --- Term query tests ---

  @Test
  public void testTermQuery_lowercaseNormalizer_queryWithLowercase() {
    // All three docs indexed "Hello World", "hello world", "HELLO WORLD" should be normalized to
    // "hello world" at index time, so querying with "hello world" matches all three.
    List<String> ids = getIdsForTermQuery("atom_lower", "hello world");
    assertEquals(3, ids.size());
    assertTrue(ids.containsAll(Arrays.asList("1", "2", "3")));
  }

  @Test
  public void testTermQuery_lowercaseNormalizer_queryWithMixedCase() {
    // Query value "Hello World" is normalized to "hello world" at query time — matches all three.
    List<String> ids = getIdsForTermQuery("atom_lower", "Hello World");
    assertEquals(3, ids.size());
    assertTrue(ids.containsAll(Arrays.asList("1", "2", "3")));
  }

  @Test
  public void testTermQuery_lowercaseNormalizer_queryWithUppercase() {
    // Query value "HELLO WORLD" is normalized to "hello world" at query time — matches all three.
    List<String> ids = getIdsForTermQuery("atom_lower", "HELLO WORLD");
    assertEquals(3, ids.size());
    assertTrue(ids.containsAll(Arrays.asList("1", "2", "3")));
  }

  @Test
  public void testTermQuery_noNormalizer_caseSensitive() {
    // Without normalizer, "Hello World" only matches doc 1, not 2 or 3.
    List<String> ids = getIdsForTermQuery("atom_no_normalizer", "Hello World");
    assertEquals(1, ids.size());
    assertTrue(ids.contains("1"));

    List<String> ids2 = getIdsForTermQuery("atom_no_normalizer", "hello world");
    assertEquals(1, ids2.size());
    assertTrue(ids2.contains("2"));
  }

  // --- Term-in-set query tests ---

  @Test
  public void testTermInSetQuery_lowercaseNormalizer() {
    // Query with mixed-case values — all should be normalized and match all three docs.
    List<String> ids = getIdsForTermInSetQuery("atom_lower", "hello world", "HELLO WORLD");
    assertEquals(3, ids.size());
    assertTrue(ids.containsAll(Arrays.asList("1", "2", "3")));
  }

  @Test
  public void testTermInSetQuery_lowercaseNormalizer_singleValue() {
    List<String> ids = getIdsForTermInSetQuery("atom_lower", "Hello World");
    assertEquals(3, ids.size());
  }

  // --- Multi-value field tests ---

  @Test
  public void testTermQuery_multiValued_normalized() {
    // All three docs have "foo" under different cases: "Foo", "foo", "FOO" — all normalize to "foo"
    List<String> ids = getIdsForTermQuery("atom_lower_multi", "Foo");
    assertEquals(3, ids.size());
    assertTrue(ids.containsAll(Arrays.asList("1", "2", "3")));

    // "FOO" normalized to "foo" — same result
    List<String> ids2 = getIdsForTermQuery("atom_lower_multi", "FOO");
    assertEquals(3, ids2.size());
    assertTrue(ids2.containsAll(Arrays.asList("1", "2", "3")));

    // "BAR" normalized to "bar" — docs 1 and 2 have "BAR"/"bar"; doc 3 has "BAZ"
    List<String> ids3 = getIdsForTermQuery("atom_lower_multi", "BAR");
    assertEquals(2, ids3.size());
    assertTrue(ids3.containsAll(Arrays.asList("1", "2")));
  }

  // --- Stored field tests ---

  @Test
  public void testStoredField_returnsNormalizedValue() {
    // Stored field should return normalized (lowercased) value regardless of input case.
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("atom_lower_stored")
                    .setQuery(Query.newBuilder().build())
                    .build());
    assertEquals(3, response.getHitsCount());
    for (Hit hit : response.getHitsList()) {
      String stored = hit.getFieldsOrThrow("atom_lower_stored").getFieldValue(0).getTextValue();
      assertEquals("Stored value should be normalized to lowercase", "hello world", stored);
    }
  }

  // --- Prefix query tests ---

  @Test
  public void testPrefixQuery_lowercaseNormalizer() {
    // "Apple", "apricot", "AVOCADO" all normalized to "apple", "apricot", "avocado"
    // Prefix "a" should match all three docs
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(
                        Query.newBuilder()
                            .setPrefixQuery(
                                PrefixQuery.newBuilder()
                                    .setField("atom_lower_prefix")
                                    .setPrefix("A")
                                    .build())
                            .build())
                    .build());
    assertEquals(3, response.getHitsCount());
  }

  @Test
  public void testPrefixQuery_lowercaseNormalizer_lowercase() {
    // Prefix "ap" (lowercase) should match "apple" and "apricot" (docs 1 and 2)
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(
                        Query.newBuilder()
                            .setPrefixQuery(
                                PrefixQuery.newBuilder()
                                    .setField("atom_lower_prefix")
                                    .setPrefix("AP")
                                    .build())
                            .build())
                    .build());
    assertEquals(2, response.getHitsCount());
  }

  // --- Range query tests ---

  @Test
  public void testRangeQuery_lowercaseNormalizer() {
    // Values indexed: "b" (doc1), "d" (doc2), "f" (doc3) — all normalized from B/D/F
    // Range [C, E] should match "d" only (doc 2)
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(
                        Query.newBuilder()
                            .setRangeQuery(
                                RangeQuery.newBuilder()
                                    .setField("atom_lower_range")
                                    .setLower("C")
                                    .setUpper("E")
                                    .build())
                            .build())
                    .build());
    assertEquals(1, response.getHitsCount());
    assertEquals(
        "2", response.getHits(0).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
  }

  // --- Custom normalizer test ---

  @Test
  public void testCustomNormalizer_lowercase() {
    // "CUSTOM" in doc 1 and 3, "custom" in doc 2 — all normalize to "custom"
    List<String> ids = getIdsForTermQuery("atom_custom_lower", "CUSTOM");
    assertEquals(3, ids.size());
    assertTrue(ids.containsAll(Arrays.asList("1", "2", "3")));
  }

  // --- Helper methods ---

  private List<String> getIdsForTermQuery(String field, String term) {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField(field).setTextValue(term).build())
                            .build())
                    .build());
    return response.getHitsList().stream()
        .map(hit -> hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue())
        .sorted()
        .collect(Collectors.toList());
  }

  private List<String> getIdsForTermInSetQuery(String field, String... terms) {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(
                        Query.newBuilder()
                            .setTermInSetQuery(
                                TermInSetQuery.newBuilder()
                                    .setField(field)
                                    .setTextTerms(
                                        TextTerms.newBuilder()
                                            .addAllTerms(Arrays.asList(terms))
                                            .build())
                                    .build())
                            .build())
                    .build());
    return response.getHitsList().stream()
        .map(hit -> hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue())
        .sorted()
        .collect(Collectors.toList());
  }
}
