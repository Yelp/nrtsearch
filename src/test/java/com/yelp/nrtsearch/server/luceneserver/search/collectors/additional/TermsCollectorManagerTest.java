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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreatorContext;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Collections;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.junit.ClassRule;
import org.junit.Test;

public class TermsCollectorManagerTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/collection/terms_building.json");
  }

  @Test
  public void testFieldNotExist() throws IOException {
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    IndexState indexState = getGlobalState().getIndex(DEFAULT_TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    try {
      s = shardState.acquire();
      CollectorCreatorContext context =
          new CollectorCreatorContext(null, indexState, shardState, indexState.getAllFields(), s);
      TermsCollector termsCollector =
          TermsCollector.newBuilder().setField("not_exist").setSize(10).build();
      try {
        TermsCollectorManager.buildManager(
            "test_collector", termsCollector, context, Collections.emptyMap());
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals("Unknown field: not_exist", e.getMessage());
      }
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  @Test
  public void testNoDocValues() throws IOException {
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    IndexState indexState = getGlobalState().getIndex(DEFAULT_TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    try {
      s = shardState.acquire();
      CollectorCreatorContext context =
          new CollectorCreatorContext(null, indexState, shardState, indexState.getAllFields(), s);
      TermsCollector termsCollector =
          TermsCollector.newBuilder().setField("no_doc_values").setSize(10).build();
      try {
        TermsCollectorManager.buildManager(
            "test_collector", termsCollector, context, Collections.emptyMap());
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(
            "Terms collection requires doc values for field: no_doc_values", e.getMessage());
      }
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }
}
