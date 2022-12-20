/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AddFieldsSimilarityTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static final List<Field> initialFields =
      List.of(
          Field.newBuilder()
              .setName("id")
              .setType(FieldType._ID)
              .setStoreDocValues(true)
              .setSearch(true)
              .build(),
          Field.newBuilder()
              .setName("field1")
              .setStoreDocValues(true)
              .setSearch(true)
              .setTokenize(true)
              .setType(FieldType.TEXT)
              .setSimilarity("classic")
              .build());

  private static final List<Field> additionalFields =
      List.of(
          Field.newBuilder()
              .setName("field2")
              .setStoreDocValues(true)
              .setSearch(true)
              .setTokenize(true)
              .setType(FieldType.TEXT)
              .setSimilarity("classic")
              .build());

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  private Similarity getSimilarity(TestServer server, String field) throws IOException {
    Similarity writerSim =
        server
            .getGlobalState()
            .getIndex("test_index")
            .getShard(0)
            .writer
            .getConfig()
            .getSimilarity();
    assertTrue(writerSim instanceof PerFieldSimilarityWrapper);
    return ((PerFieldSimilarityWrapper) writerSim).get(field);
  }

  @Test
  public void testAddFieldsPreIndexStart() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    primaryServer.createIndex("test_index");
    primaryServer.registerFields("test_index", initialFields);
    primaryServer.registerFields("test_index", additionalFields);
    primaryServer.startPrimaryIndex("test_index", -1, null);
    assertEquals(BM25Similarity.class, getSimilarity(primaryServer, "id").getClass());
    assertEquals(ClassicSimilarity.class, getSimilarity(primaryServer, "field1").getClass());
    assertEquals(ClassicSimilarity.class, getSimilarity(primaryServer, "field2").getClass());
  }

  @Test
  public void testAddFieldsPostIndexStart() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    primaryServer.createIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);
    primaryServer.registerFields("test_index", initialFields);
    primaryServer.registerFields("test_index", additionalFields);
    assertEquals(BM25Similarity.class, getSimilarity(primaryServer, "id").getClass());
    assertEquals(ClassicSimilarity.class, getSimilarity(primaryServer, "field1").getClass());
    assertEquals(ClassicSimilarity.class, getSimilarity(primaryServer, "field2").getClass());
  }

  @Test
  public void testAddFieldsSplitIndexStart() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    primaryServer.createIndex("test_index");
    primaryServer.registerFields("test_index", initialFields);
    primaryServer.startPrimaryIndex("test_index", -1, null);
    primaryServer.registerFields("test_index", additionalFields);
    assertEquals(BM25Similarity.class, getSimilarity(primaryServer, "id").getClass());
    assertEquals(ClassicSimilarity.class, getSimilarity(primaryServer, "field1").getClass());
    assertEquals(ClassicSimilarity.class, getSimilarity(primaryServer, "field2").getClass());
  }
}
