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
package com.yelp.nrtsearch.server.luceneserver.search;

import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder;
import com.yelp.nrtsearch.server.grpc.SearchResponse.SearchState;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.DocCollector;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.ClassRule;
import org.junit.Test;

public class SearchContextTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public static class DummyCollector implements DocCollector {

    @Override
    public CollectorManager<? extends Collector, ? extends TopDocs> getManager() {
      return null;
    }

    @Override
    public void fillHitRanking(Builder hitResponse, ScoreDoc scoreDoc) {}

    @Override
    public void fillLastHit(SearchState.Builder stateBuilder, ScoreDoc lastHit) {}
  }

  @Override
  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsBasic.json");
  }

  @Test
  public void testValid() throws Exception {
    SearchContext.newBuilder()
        .setIndexState(getGlobalState().getIndex(DEFAULT_TEST_INDEX))
        .setShardState(getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0))
        .setSearcherAndTaxonomy(new SearcherAndTaxonomy(null, null))
        .setResponseBuilder(SearchResponse.newBuilder())
        .setTimestampSec(1)
        .setStartHit(0)
        .setTopHits(10)
        .setQueryFields(Collections.emptyMap())
        .setRetrieveFields(Collections.emptyMap())
        .setQuery(new MatchAllDocsQuery())
        .setCollector(new DummyCollector())
        .build(true);
  }

  @Test
  public void testNoValidate() {
    SearchContext.newBuilder().build(false);
  }

  @Test(expected = NullPointerException.class)
  public void testEmpty() {
    SearchContext.newBuilder().build(true);
  }

  @Test(expected = NullPointerException.class)
  public void testMissingIndexState() throws Exception {
    SearchContext.newBuilder()
        .setShardState(getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0))
        .setSearcherAndTaxonomy(new SearcherAndTaxonomy(null, null))
        .setResponseBuilder(SearchResponse.newBuilder())
        .setTimestampSec(1)
        .setStartHit(0)
        .setTopHits(10)
        .setQueryFields(Collections.emptyMap())
        .setRetrieveFields(Collections.emptyMap())
        .setQuery(new MatchAllDocsQuery())
        .setCollector(new DummyCollector())
        .build(true);
  }

  @Test(expected = NullPointerException.class)
  public void testMissingShardState() throws Exception {
    SearchContext.newBuilder()
        .setIndexState(getGlobalState().getIndex(DEFAULT_TEST_INDEX))
        .setSearcherAndTaxonomy(new SearcherAndTaxonomy(null, null))
        .setResponseBuilder(SearchResponse.newBuilder())
        .setTimestampSec(1)
        .setStartHit(0)
        .setTopHits(10)
        .setQueryFields(Collections.emptyMap())
        .setRetrieveFields(Collections.emptyMap())
        .setQuery(new MatchAllDocsQuery())
        .setCollector(new DummyCollector())
        .build(true);
  }

  @Test(expected = NullPointerException.class)
  public void testMissingSearcher() throws Exception {
    SearchContext.newBuilder()
        .setIndexState(getGlobalState().getIndex(DEFAULT_TEST_INDEX))
        .setShardState(getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0))
        .setResponseBuilder(SearchResponse.newBuilder())
        .setTimestampSec(1)
        .setStartHit(0)
        .setTopHits(10)
        .setQueryFields(Collections.emptyMap())
        .setRetrieveFields(Collections.emptyMap())
        .setQuery(new MatchAllDocsQuery())
        .setCollector(new DummyCollector())
        .build(true);
  }

  @Test(expected = NullPointerException.class)
  public void testMissingResponseBuilder() throws Exception {
    SearchContext.newBuilder()
        .setIndexState(getGlobalState().getIndex(DEFAULT_TEST_INDEX))
        .setShardState(getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0))
        .setSearcherAndTaxonomy(new SearcherAndTaxonomy(null, null))
        .setTimestampSec(1)
        .setStartHit(0)
        .setTopHits(10)
        .setQueryFields(Collections.emptyMap())
        .setRetrieveFields(Collections.emptyMap())
        .setQuery(new MatchAllDocsQuery())
        .setCollector(new DummyCollector())
        .build(true);
  }

  @Test(expected = IllegalStateException.class)
  public void testMissingTimestamp() throws Exception {
    SearchContext.newBuilder()
        .setIndexState(getGlobalState().getIndex(DEFAULT_TEST_INDEX))
        .setShardState(getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0))
        .setSearcherAndTaxonomy(new SearcherAndTaxonomy(null, null))
        .setResponseBuilder(SearchResponse.newBuilder())
        .setStartHit(0)
        .setTopHits(10)
        .setQueryFields(Collections.emptyMap())
        .setRetrieveFields(Collections.emptyMap())
        .setQuery(new MatchAllDocsQuery())
        .setCollector(new DummyCollector())
        .build(true);
  }

  @Test(expected = IllegalStateException.class)
  public void testMissingStartHit() throws Exception {
    SearchContext.newBuilder()
        .setIndexState(getGlobalState().getIndex(DEFAULT_TEST_INDEX))
        .setShardState(getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0))
        .setSearcherAndTaxonomy(new SearcherAndTaxonomy(null, null))
        .setResponseBuilder(SearchResponse.newBuilder())
        .setTimestampSec(1)
        .setTopHits(10)
        .setQueryFields(Collections.emptyMap())
        .setRetrieveFields(Collections.emptyMap())
        .setQuery(new MatchAllDocsQuery())
        .setCollector(new DummyCollector())
        .build(true);
  }

  @Test(expected = IllegalStateException.class)
  public void testMissingTopHits() throws Exception {
    SearchContext.newBuilder()
        .setIndexState(getGlobalState().getIndex(DEFAULT_TEST_INDEX))
        .setShardState(getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0))
        .setSearcherAndTaxonomy(new SearcherAndTaxonomy(null, null))
        .setResponseBuilder(SearchResponse.newBuilder())
        .setTimestampSec(1)
        .setStartHit(0)
        .setQueryFields(Collections.emptyMap())
        .setRetrieveFields(Collections.emptyMap())
        .setQuery(new MatchAllDocsQuery())
        .setCollector(new DummyCollector())
        .build(true);
  }

  @Test(expected = NullPointerException.class)
  public void testMissingQueryFields() throws Exception {
    SearchContext.newBuilder()
        .setIndexState(getGlobalState().getIndex(DEFAULT_TEST_INDEX))
        .setShardState(getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0))
        .setSearcherAndTaxonomy(new SearcherAndTaxonomy(null, null))
        .setResponseBuilder(SearchResponse.newBuilder())
        .setTimestampSec(1)
        .setStartHit(0)
        .setTopHits(10)
        .setRetrieveFields(Collections.emptyMap())
        .setQuery(new MatchAllDocsQuery())
        .setCollector(new DummyCollector())
        .build(true);
  }

  @Test(expected = NullPointerException.class)
  public void testMissingRetrieveFields() throws Exception {
    SearchContext.newBuilder()
        .setIndexState(getGlobalState().getIndex(DEFAULT_TEST_INDEX))
        .setShardState(getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0))
        .setSearcherAndTaxonomy(new SearcherAndTaxonomy(null, null))
        .setResponseBuilder(SearchResponse.newBuilder())
        .setTimestampSec(1)
        .setStartHit(0)
        .setTopHits(10)
        .setQueryFields(Collections.emptyMap())
        .setQuery(new MatchAllDocsQuery())
        .setCollector(new DummyCollector())
        .build(true);
  }

  @Test(expected = NullPointerException.class)
  public void testMissingQuery() throws Exception {
    SearchContext.newBuilder()
        .setIndexState(getGlobalState().getIndex(DEFAULT_TEST_INDEX))
        .setShardState(getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0))
        .setSearcherAndTaxonomy(new SearcherAndTaxonomy(null, null))
        .setResponseBuilder(SearchResponse.newBuilder())
        .setTimestampSec(1)
        .setStartHit(0)
        .setTopHits(10)
        .setQueryFields(Collections.emptyMap())
        .setRetrieveFields(Collections.emptyMap())
        .setCollector(new DummyCollector())
        .build(true);
  }

  @Test(expected = NullPointerException.class)
  public void testMissingCollector() throws Exception {
    SearchContext.newBuilder()
        .setIndexState(getGlobalState().getIndex(DEFAULT_TEST_INDEX))
        .setShardState(getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0))
        .setSearcherAndTaxonomy(new SearcherAndTaxonomy(null, null))
        .setResponseBuilder(SearchResponse.newBuilder())
        .setTimestampSec(1)
        .setStartHit(0)
        .setTopHits(10)
        .setQueryFields(Collections.emptyMap())
        .setRetrieveFields(Collections.emptyMap())
        .setQuery(new MatchAllDocsQuery())
        .build(true);
  }
}
