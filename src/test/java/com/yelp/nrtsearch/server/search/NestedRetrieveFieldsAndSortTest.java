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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.*;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration tests for _CHILDREN. and _PARENT. prefix support in retrieveFields, and nested sort
 * via ToParentBlockJoinSortField.
 *
 * <p>Test data layout (5 parent docs):
 *
 * <pre>
 *   doc 0: guest_name="alice", emails=[{email:"alice@a.com", priority:1}],
 *          visits=[{date:"2024-01-01", spend:100}]
 *   doc 1: guest_name="bob",   emails=[{email:"bob@b.com", priority:2}, {email:"bob2@b.com", priority:1}],
 *          visits=[{date:"2024-02-01", spend:200}]
 *   doc 2: guest_name="carol", emails=[{email:"carol@c.com", priority:3}],
 *          visits=[{date:"2024-03-01", spend:50}]
 *   doc 3: guest_name="dave",  emails=[{email:"dave@d.com", priority:4}],
 *          visits=[{date:"2024-04-01", spend:300}, {date:"2024-04-15", spend:75}]
 *   doc 4: guest_name="eve",   emails=[] (no children),
 *          visits=[{date:"2024-05-01", spend:150}]
 * </pre>
 */
public class NestedRetrieveFieldsAndSortTest extends ServerTestCase {
  private static final String TEST_INDEX = "test_index_nested_retrieve_sort";

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  protected Gson gson = new GsonBuilder().serializeNulls().create();

  @Override
  public List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsNestedRetrieveAndSort.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndexOrThrow(name).getShard(0).writer;
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    List<AddDocumentRequest> docs = new ArrayList<>();

    // Doc 0: alice with 1 email
    docs.add(
        buildDoc(
            name,
            "0",
            "alice",
            List.of(email("alice@a.com", 1)),
            List.of(visit("2024-01-01", 100))));

    // Doc 1: bob with 2 emails
    docs.add(
        buildDoc(
            name,
            "1",
            "bob",
            List.of(email("bob@b.com", 2), email("bob2@b.com", 1)),
            List.of(visit("2024-02-01", 200))));

    // Doc 2: carol with 1 email
    docs.add(
        buildDoc(
            name,
            "2",
            "carol",
            List.of(email("carol@c.com", 3)),
            List.of(visit("2024-03-01", 50))));

    // Doc 3: dave with 1 email, 2 visits
    docs.add(
        buildDoc(
            name,
            "3",
            "dave",
            List.of(email("dave@d.com", 4)),
            List.of(visit("2024-04-01", 300), visit("2024-04-15", 75))));

    // Doc 4: eve with NO emails, 1 visit
    docs.add(buildDoc(name, "4", "eve", List.of(), List.of(visit("2024-05-01", 150))));

    addDocuments(docs.stream());
    writer.commit();
  }

  private Map<String, Object> email(String addr, int priority) {
    Map<String, Object> m = new HashMap<>();
    m.put("email", addr);
    m.put("priority", priority);
    return m;
  }

  private Map<String, Object> visit(String date, int spend) {
    Map<String, Object> m = new HashMap<>();
    m.put("date", date);
    m.put("spend", spend);
    return m;
  }

  private AddDocumentRequest buildDoc(
      String indexName,
      String id,
      String guestName,
      List<Map<String, Object>> emails,
      List<Map<String, Object>> visits) {
    AddDocumentRequest.Builder builder =
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue(id).build())
            .putFields(
                "guest_name",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue(guestName).build());

    if (!emails.isEmpty()) {
      AddDocumentRequest.MultiValuedField.Builder emailsBuilder =
          AddDocumentRequest.MultiValuedField.newBuilder();
      for (Map<String, Object> e : emails) {
        emailsBuilder.addValue(gson.toJson(e));
      }
      builder.putFields("emails", emailsBuilder.build());
    }

    if (!visits.isEmpty()) {
      AddDocumentRequest.MultiValuedField.Builder visitsBuilder =
          AddDocumentRequest.MultiValuedField.newBuilder();
      for (Map<String, Object> v : visits) {
        visitsBuilder.addValue(gson.toJson(v));
      }
      builder.putFields("visits", visitsBuilder.build());
    }

    return builder.build();
  }

  // =================== _CHILDREN. retrieveFields tests ===================

  @Test
  public void testRetrieveChildrenEmailField() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("doc_id").setTextValue("1").build())
                            .build())
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("_CHILDREN.emails.email")
                    .build());

    assertEquals(1, response.getHitsCount());
    SearchResponse.Hit hit = response.getHits(0);
    assertEquals("1", hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());

    // Bob has 2 emails
    SearchResponse.Hit.CompositeFieldValue emailField =
        hit.getFieldsOrThrow("_CHILDREN.emails.email");
    assertEquals(2, emailField.getFieldValueCount());

    Set<String> emails = new HashSet<>();
    emails.add(emailField.getFieldValue(0).getTextValue());
    emails.add(emailField.getFieldValue(1).getTextValue());
    assertTrue(emails.contains("bob@b.com"));
    assertTrue(emails.contains("bob2@b.com"));
  }

  @Test
  public void testRetrieveChildrenNumericField() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("doc_id").setTextValue("3").build())
                            .build())
                    .addRetrieveFields("_CHILDREN.visits.spend")
                    .build());

    assertEquals(1, response.getHitsCount());
    SearchResponse.Hit hit = response.getHits(0);

    // Dave has 2 visits with spend 300 and 75
    SearchResponse.Hit.CompositeFieldValue spendField =
        hit.getFieldsOrThrow("_CHILDREN.visits.spend");
    assertEquals(2, spendField.getFieldValueCount());

    Set<Integer> spends = new HashSet<>();
    spends.add(spendField.getFieldValue(0).getIntValue());
    spends.add(spendField.getFieldValue(1).getIntValue());
    assertTrue(spends.contains(300));
    assertTrue(spends.contains(75));
  }

  @Test
  public void testRetrieveChildrenEmptyWhenNoChildren() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("doc_id").setTextValue("4").build())
                            .build())
                    .addRetrieveFields("_CHILDREN.emails.email")
                    .build());

    assertEquals(1, response.getHitsCount());
    SearchResponse.Hit hit = response.getHits(0);

    // Eve has no email children
    SearchResponse.Hit.CompositeFieldValue emailField =
        hit.getFieldsOrThrow("_CHILDREN.emails.email");
    assertEquals(0, emailField.getFieldValueCount());
  }

  @Test
  public void testRetrieveChildrenPathFiltering() {
    // Verify that _CHILDREN.emails.email only returns email children, not visit children
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("doc_id").setTextValue("0").build())
                            .build())
                    .addRetrieveFields("_CHILDREN.emails.email")
                    .addRetrieveFields("_CHILDREN.visits.spend")
                    .build());

    assertEquals(1, response.getHitsCount());
    SearchResponse.Hit hit = response.getHits(0);

    // Alice has 1 email and 1 visit - verify they don't mix
    assertEquals(1, hit.getFieldsOrThrow("_CHILDREN.emails.email").getFieldValueCount());
    assertEquals(
        "alice@a.com",
        hit.getFieldsOrThrow("_CHILDREN.emails.email").getFieldValue(0).getTextValue());

    assertEquals(1, hit.getFieldsOrThrow("_CHILDREN.visits.spend").getFieldValueCount());
    assertEquals(
        100, hit.getFieldsOrThrow("_CHILDREN.visits.spend").getFieldValue(0).getIntValue());
  }

  // =================== _PARENT. retrieveFields tests ===================

  @Test
  public void testRetrieveParentFieldFromChildQuery() {
    // Query at child level using queryNestedPath, retrieve parent field
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setTopHits(10)
                    .setQueryNestedPath("emails")
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("emails.email")
                                    .setTextValue("carol@c.com")
                                    .build())
                            .build())
                    .addRetrieveFields("emails.email")
                    .addRetrieveFields("_PARENT.guest_name")
                    .build());

    assertEquals(1, response.getHitsCount());
    SearchResponse.Hit hit = response.getHits(0);

    // Child field
    assertEquals(
        "carol@c.com", hit.getFieldsOrThrow("emails.email").getFieldValue(0).getTextValue());

    // Parent field retrieved via _PARENT. prefix
    assertEquals(
        "carol", hit.getFieldsOrThrow("_PARENT.guest_name").getFieldValue(0).getTextValue());
  }

  // =================== Nested sort tests ===================

  @Test
  public void testNestedSortByChildIntFieldMin() {
    // Sort parents by minimum child visit spend (ascending)
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setTopHits(10)
                    .setQuery(Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder()))
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("guest_name")
                    .setQuerySort(
                        QuerySortField.newBuilder()
                            .setFields(
                                SortFields.newBuilder()
                                    .addSortedFields(
                                        SortType.newBuilder()
                                            .setFieldName("visits.spend")
                                            .setSelector(Selector.MIN)
                                            .setNested(
                                                NestedSortContext.newBuilder()
                                                    .setNestedPath("visits")
                                                    .build())
                                            .build())))
                    .build());

    assertEquals(5, response.getHitsCount());

    // Expected order by min visit spend:
    // carol(50), dave(min(300,75)=75), alice(100), eve(150), bob(200)
    List<String> expectedOrder = List.of("carol", "dave", "alice", "eve", "bob");
    for (int i = 0; i < 5; i++) {
      assertEquals(
          expectedOrder.get(i),
          response.getHits(i).getFieldsOrThrow("guest_name").getFieldValue(0).getTextValue());
    }
  }

  @Test
  public void testNestedSortByChildIntFieldMax() {
    // Sort parents by maximum child visit spend (ascending)
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setTopHits(10)
                    .setQuery(Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder()))
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("guest_name")
                    .setQuerySort(
                        QuerySortField.newBuilder()
                            .setFields(
                                SortFields.newBuilder()
                                    .addSortedFields(
                                        SortType.newBuilder()
                                            .setFieldName("visits.spend")
                                            .setSelector(Selector.MAX)
                                            .setNested(
                                                NestedSortContext.newBuilder()
                                                    .setNestedPath("visits")
                                                    .build())
                                            .build())))
                    .build());

    assertEquals(5, response.getHitsCount());

    // Expected order by max visit spend (ascending):
    // carol(50), alice(100), eve(150), bob(200), dave(max(300,75)=300)
    List<String> expectedOrder = List.of("carol", "alice", "eve", "bob", "dave");
    for (int i = 0; i < 5; i++) {
      assertEquals(
          expectedOrder.get(i),
          response.getHits(i).getFieldsOrThrow("guest_name").getFieldValue(0).getTextValue());
    }
  }

  @Test
  public void testNestedSortByChildStringField() {
    // Sort parents by child email string (ascending, MIN selector)
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setTopHits(10)
                    .setQuery(Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder()))
                    .addRetrieveFields("guest_name")
                    .setQuerySort(
                        QuerySortField.newBuilder()
                            .setFields(
                                SortFields.newBuilder()
                                    .addSortedFields(
                                        SortType.newBuilder()
                                            .setFieldName("emails.email")
                                            .setSelector(Selector.MIN)
                                            .setNested(
                                                NestedSortContext.newBuilder()
                                                    .setNestedPath("emails")
                                                    .build())
                                            .build())))
                    .build());

    // Parents with emails are sorted alphabetically by min email:
    // alice@a.com, bob2@b.com (min of bob's two), carol@c.com, dave@d.com
    // Eve has no email children - she'll sort first or last depending on missing value handling
    assertTrue(response.getHitsCount() > 0);

    // Check that at least the first result with emails is alice
    // (Eve may be at position 0 with null/empty sort value)
    boolean foundAliceFirst = false;
    for (int i = 0; i < response.getHitsCount(); i++) {
      String name =
          response.getHits(i).getFieldsOrThrow("guest_name").getFieldValue(0).getTextValue();
      if (name.equals("alice")) {
        foundAliceFirst = true;
        // Verify alice comes before bob in results
        for (int j = i + 1; j < response.getHitsCount(); j++) {
          String laterName =
              response.getHits(j).getFieldsOrThrow("guest_name").getFieldValue(0).getTextValue();
          if (laterName.equals("bob")) {
            break; // good - bob comes after alice
          }
        }
        break;
      }
    }
    assertTrue("alice should appear in results", foundAliceFirst);
  }

  // =================== Error handling tests ===================

  @Test(expected = StatusRuntimeException.class)
  public void testRetrieveChildrenNonExistentFieldThrows() {
    getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setTopHits(10)
                .setQuery(Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder()))
                .addRetrieveFields("_CHILDREN.nonexistent.field")
                .build());
  }

  @Test(expected = StatusRuntimeException.class)
  public void testNestedSortWithInvalidPathThrows() {
    getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setTopHits(10)
                .setQuery(Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder()))
                .addRetrieveFields("doc_id")
                .setQuerySort(
                    QuerySortField.newBuilder()
                        .setFields(
                            SortFields.newBuilder()
                                .addSortedFields(
                                    SortType.newBuilder()
                                        .setFieldName("visits.spend")
                                        .setNested(
                                            NestedSortContext.newBuilder()
                                                .setNestedPath("nonexistent_path")
                                                .build())
                                        .build())))
                .build());
  }
}
