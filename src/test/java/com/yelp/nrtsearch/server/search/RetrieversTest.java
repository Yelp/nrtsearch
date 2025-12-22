/*
 * Copyright 2025 Yelp Inc.
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

import static org.assertj.core.api.Assertions.*;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.ClassRule;
import org.junit.Test;

public class RetrieversTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/Retrievers.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    AtomicInteger id = new AtomicInteger(0);
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "doc_id",
                MultiValuedField.newBuilder()
                    .addValue(Integer.toString(id.getAndIncrement()))
                    .build())
            .putFields(
                "field_A",
                MultiValuedField.newBuilder()
                    .addAllValue(List.of("apple", "pear", "banana"))
                    .build())
            .putFields(
                "field_B",
                MultiValuedField.newBuilder()
                    .addAllValue(List.of("diamond", "gold", "pearl"))
                    .build())
            .putFields(
                "field_C",
                MultiValuedField.newBuilder()
                    .addAllValue(List.of("apple", "pear", "banana"))
                    .build())
            .build();
    docs.add(request);

    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "doc_id",
                MultiValuedField.newBuilder()
                    .addValue(Integer.toString(id.getAndIncrement()))
                    .build())
            .putFields(
                "field_A",
                MultiValuedField.newBuilder()
                    .addAllValue(List.of("grapefruit", "dragonfruit", "passionfruit"))
                    .build())
            .putFields(
                "field_B",
                MultiValuedField.newBuilder()
                    .addAllValue(List.of("diamond", "silver", "ruby"))
                    .build())
            .putFields(
                "field_C",
                MultiValuedField.newBuilder()
                    .addAllValue(List.of("grapefruit", "dragonfruit", "passionfruit"))
                    .build())
            .build();
    docs.add(request);

    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "doc_id",
                MultiValuedField.newBuilder()
                    .addValue(Integer.toString(id.getAndIncrement()))
                    .build())
            .putFields(
                "field_A",
                MultiValuedField.newBuilder()
                    .addAllValue(List.of("watermelon", "cantaloupe", "dinomelon"))
                    .build())
            .putFields(
                "field_B",
                MultiValuedField.newBuilder()
                    .addAllValue(List.of("watermelon", "cantaloupe", "dinomelon"))
                    .build())
            .putFields(
                "field_C",
                MultiValuedField.newBuilder()
                    .addAllValue(List.of("watermelon", "cantaloupe", "dinomelon"))
                    .build())
            .build();
    docs.add(request);

    addDocuments(docs.stream());
  }

  @Test
  public void testBasic() {
    Query query1 =
        Query.newBuilder()
            .setTermQuery(TermQuery.newBuilder().setField("field_A").setTextValue("apple"))
            .build();
    Query query2 =
        Query.newBuilder()
            .setTermQuery(TermQuery.newBuilder().setField("field_A").setTextValue("passionfruit"))
            .build();

    //    SearchRequest request0 =
    // SearchRequest.newBuilder().setIndexName("test_index").setTopHits(10).addRetrieveFields("*").build();
    //    getGrpcServer().getBlockingStub().search(request0);

    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .addRetrievers(
                Retriever.newBuilder()
                    .setQuery(query1)
                    .setStartHit(0)
                    .setTopHits(1)
                    .setJoinMethod(Retriever.JoinMethod.OR)
                    .build())
            .addRetrievers(
                Retriever.newBuilder()
                    .setQuery(query2)
                    .setStartHit(0)
                    .setTopHits(1)
                    .setJoinMethod(Retriever.JoinMethod.OR)
                    .build())
            .putCollectors(
                    "jewellery",
                    Collector.newBuilder()
                            .setTerms(
                                    TermsCollector.newBuilder()
                                            .setField("field_B")
                                            .setSize(10)
                                            .build()
                            )
                            .build()
            )
            .setTopHits(10)
            .build();
    getGrpcServer().getBlockingStub().multiRetrievers(request);
  }
}
