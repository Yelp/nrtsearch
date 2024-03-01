/*
 * Copyright 2023 Yelp Inc.
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
package org.apache.lucene.search;

import static com.yelp.nrtsearch.server.luceneserver.field.VectorFieldDefTest.VECTOR_SEARCH_INDEX_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.junit.Test;

public class NrtsearchKnnCollectorTest extends ServerTestCase {

  @Override
  protected List<String> getIndices() {
    return List.of(VECTOR_SEARCH_INDEX_NAME);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsVectorSearch.json");
  }

  @Test
  public void testFieldNotExist() throws IOException {
    IndexState indexState = getIndexState();
    SearcherAndTaxonomy s = null;
    try {
      s = indexState.getShard(0).acquire();
      new NrtsearchKnnCollector(
          KnnQuery.newBuilder().setField("unknown").build(), indexState, s.searcher);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "field \"unknown\" is unknown: it was not registered with registerField", e.getMessage());
    } finally {
      if (s != null) {
        indexState.getShard(0).release(s);
      }
    }
  }

  @Test
  public void testFieldNotVector() throws IOException {
    IndexState indexState = getIndexState();
    SearcherAndTaxonomy s = null;
    try {
      s = indexState.getShard(0).acquire();
      new NrtsearchKnnCollector(
          KnnQuery.newBuilder().setField("filter").build(), indexState, s.searcher);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Field is not a vector: filter", e.getMessage());
    } finally {
      if (s != null) {
        indexState.getShard(0).release(s);
      }
    }
  }

  @Test
  public void testFieldNotSearchable() throws IOException {
    IndexState indexState = getIndexState();
    SearcherAndTaxonomy s = null;
    try {
      s = indexState.getShard(0).acquire();
      new NrtsearchKnnCollector(
          KnnQuery.newBuilder().setField("vector_not_search").build(), indexState, s.searcher);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector field is not searchable: vector_not_search", e.getMessage());
    } finally {
      if (s != null) {
        indexState.getShard(0).release(s);
      }
    }
  }

  @Test
  public void testInvalidVectorSize() throws IOException {
    IndexState indexState = getIndexState();
    SearcherAndTaxonomy s = null;
    try {
      s = indexState.getShard(0).acquire();
      new NrtsearchKnnCollector(
          KnnQuery.newBuilder()
              .setField("vector_cosine")
              .setK(5)
              .setNumCandidates(10)
              .addAllQueryVector(List.of(1.0f, 1.0f))
              .build(),
          indexState,
          s.searcher);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid query vector size, expected: 3, found: 2", e.getMessage());
    } finally {
      if (s != null) {
        indexState.getShard(0).release(s);
      }
    }
  }

  @Test
  public void testInvalidK() throws IOException {
    IndexState indexState = getIndexState();
    SearcherAndTaxonomy s = null;
    try {
      s = indexState.getShard(0).acquire();
      new NrtsearchKnnCollector(
          KnnQuery.newBuilder()
              .setField("vector_cosine")
              .setNumCandidates(10)
              .addAllQueryVector(List.of(1.0f, 1.0f, 1.0f))
              .build(),
          indexState,
          s.searcher);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector search k must be >= 1", e.getMessage());
    } finally {
      if (s != null) {
        indexState.getShard(0).release(s);
      }
    }
  }

  @Test
  public void testNumCandidatesLessThanK() throws IOException {
    IndexState indexState = getIndexState();
    SearcherAndTaxonomy s = null;
    try {
      s = indexState.getShard(0).acquire();
      new NrtsearchKnnCollector(
          KnnQuery.newBuilder()
              .setField("vector_cosine")
              .setK(15)
              .setNumCandidates(10)
              .addAllQueryVector(List.of(1.0f, 1.0f, 1.0f))
              .build(),
          indexState,
          s.searcher);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector search numCandidates must be >= k", e.getMessage());
    } finally {
      if (s != null) {
        indexState.getShard(0).release(s);
      }
    }
  }

  @Test
  public void testNumCandidatesMaxLimit() throws IOException {
    IndexState indexState = getIndexState();
    SearcherAndTaxonomy s = null;
    try {
      s = indexState.getShard(0).acquire();
      new NrtsearchKnnCollector(
          KnnQuery.newBuilder()
              .setField("vector_cosine")
              .setK(15)
              .setNumCandidates(10001)
              .addAllQueryVector(List.of(1.0f, 1.0f, 1.0f))
              .build(),
          indexState,
          s.searcher);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector search numCandidates > 10000", e.getMessage());
    } finally {
      if (s != null) {
        indexState.getShard(0).release(s);
      }
    }
  }

  private IndexState getIndexState() throws IOException {
    return getGlobalState().getIndex(VECTOR_SEARCH_INDEX_NAME);
  }
}
