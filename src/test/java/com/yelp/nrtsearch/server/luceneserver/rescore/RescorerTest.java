/*
 * Copyright 2021 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.rescore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.PluginRescorer;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.Rescorer;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.RescorerPlugin;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.junit.ClassRule;
import org.junit.Test;

public class RescorerTest extends ServerTestCase {
  private static final String TEST_INDEX = "test_index";
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/rescore/RescoreRegisterFields.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    // create a shuffled list of ids
    List<Integer> idList = new ArrayList<>();
    for (int i = 0; i < NUM_DOCS; ++i) {
      idList.add(i);
    }
    Collections.shuffle(idList);

    // add documents one chunk at a time to ensure multiple index segments
    List<AddDocumentRequest> requestChunk = new ArrayList<>();
    for (Integer id : idList) {
      requestChunk.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "int_score",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(NUM_DOCS - id))
                      .build())
              .putFields(
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .build());

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  static class TestRescorerPlugin extends Plugin implements RescorerPlugin {
    TestRescorerPlugin() {}

    public Map<String, RescorerProvider<? extends RescoreOperation>> getRescorers() {
      Map<String, RescorerProvider<? extends RescoreOperation>> rescorers = new HashMap<>();
      rescorers.put("test_rescorer", TestRescoreOperation::new);
      return rescorers;
    }

    static class TestRescoreOperation implements RescoreOperation {
      public TestRescoreOperation(Map<String, Object> params) {}

      @Override
      public TopDocs rescore(TopDocs hits, RescoreContext context) throws IOException {
        List<DocAndScore> docAndScores = new ArrayList<>();
        for (ScoreDoc doc : hits.scoreDocs) {
          List<LeafReaderContext> leaves =
              context
                  .getSearchContext()
                  .getSearcherAndTaxonomy()
                  .searcher
                  .getIndexReader()
                  .leaves();
          int leafIndex = ReaderUtil.subIndex(doc.doc, leaves);
          LeafReaderContext leaf = leaves.get(leafIndex);
          @SuppressWarnings("unchecked")
          LoadedDocValues<Integer> docValues =
              ((LoadedDocValues<Integer>)
                  ((IndexableFieldDef)
                          context.getSearchContext().getIndexState().getField("int_score"))
                      .getDocValues(leaf));
          docValues.setDocId(doc.doc - leaf.docBase);
          int score = docValues.get(0);
          docAndScores.add(new DocAndScore(score, doc));
        }
        docAndScores.sort(Comparator.comparingInt(d -> d.score));
        ScoreDoc[] newOrder = new ScoreDoc[context.getWindowSize()];
        int docIndex = 0;
        for (int i = 0; i < context.getWindowSize(); i++) {
          newOrder[i] = docAndScores.get(docIndex).doc;
          docIndex += 2;
        }
        return new TopDocs(new TotalHits(context.getWindowSize(), Relation.EQUAL_TO), newOrder);
      }

      static class DocAndScore {
        final int score;
        final ScoreDoc doc;

        DocAndScore(int score, ScoreDoc doc) {
          this.score = score;
          this.doc = doc;
        }
      }
    }
  }

  @Override
  protected List<Plugin> getPlugins(LuceneServerConfiguration configuration) {
    return Collections.singletonList(new TestRescorerPlugin());
  }

  @Test
  public void testUnknownRescorer() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(10)
            .setStartHit(0)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("int_score")
            .setQuery(
                Query.newBuilder()
                    .setRangeQuery(
                        RangeQuery.newBuilder()
                            .setField("int_field")
                            .setLower("5")
                            .setUpper("10")
                            .build())
                    .build())
            .addRescorers(
                Rescorer.newBuilder()
                    .setPluginRescorer(PluginRescorer.newBuilder().setName("invalid").build())
                    .build())
            .build();
    try {
      getGrpcServer().getBlockingStub().search(request);
      fail();
    } catch (StatusRuntimeException e) {
      String expectedMessage = "Invalid rescorer name: invalid, must be one of: [test_rescorer]";
      assertTrue(e.getMessage().contains(expectedMessage));
    }
  }

  @Test
  public void testCustomRescorer() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(10)
            .setStartHit(0)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("int_score")
            .setQuery(
                Query.newBuilder()
                    .setRangeQuery(
                        RangeQuery.newBuilder()
                            .setField("int_field")
                            .setLower("5")
                            .setUpper("10")
                            .build())
                    .build())
            .addRescorers(
                Rescorer.newBuilder()
                    .setWindowSize(3)
                    .setPluginRescorer(PluginRescorer.newBuilder().setName("test_rescorer").build())
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    assertEquals(3, response.getHitsCount());
    assertEquals(
        90, response.getHits(0).getFieldsOrThrow("int_score").getFieldValue(0).getIntValue());
    assertEquals(
        92, response.getHits(1).getFieldsOrThrow("int_score").getFieldValue(0).getIntValue());
    assertEquals(
        94, response.getHits(2).getFieldsOrThrow("int_score").getFieldValue(0).getIntValue());
  }
}
