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
package com.yelp.nrtsearch.server.warming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.Test;

public class TemplateWarmingQueryGeneratorTest {

  private static final double[][] SEED_POINTS = {
    {37.77, -122.41}, {40.71, -74.00}, {34.05, -118.24}, {41.88, -87.63}, {29.76, -95.37},
    {47.61, -122.33}, {39.74, -104.99}, {25.76, -80.19}, {42.36, -71.06}, {33.45, -112.07}
  };
  private static final double[][] POOL_POINTS = {
    {45.52, -122.68}, {32.72, -117.16}, {36.17, -115.14}, {38.58, -121.49}
  };

  /**
   * Real-looking seed: constant filter, a term set, a geo radius and a script whose params repeat
   * the geo centre, sorted by the script.
   */
  private static SearchRequest seed(int i) throws Exception {
    double lat = SEED_POINTS[i][0], lon = SEED_POINTS[i][1];
    String json =
        """
        {"indexName": "test_index", "topHits": 50, "retrieveFields": ["id"],
         "virtualFields": [{"name": "distance", "script": {"lang": "js", "source": "LAT + LON",
           "params": {"LAT": {"doubleValue": %s}, "LON": {"doubleValue": %s}}}}],
         "query": {"booleanQuery": {"clauses": [
           {"occur": "FILTER", "query": {"termQuery": {"field": "is_closed", "booleanValue": false}}},
           {"occur": "FILTER", "query": {"termInSetQuery": {"field": "place_ids",
             "textTerms": {"terms": ["p%d", "q%d"]}}}},
           {"occur": "FILTER", "query": {"geoRadiusQuery": {"field": "location",
             "center": {"latitude": %s, "longitude": %s}, "radius": "10 mi"}}}]}}}
        """
            .formatted(lat, lon, i, i, lat, lon);
    SearchRequest.Builder builder = SearchRequest.newBuilder();
    JsonFormat.parser().merge(json, builder);
    return builder.build();
  }

  private static List<SearchRequest> seeds() throws Exception {
    List<SearchRequest> seeds = new ArrayList<>();
    for (int i = 0; i < SEED_POINTS.length; ++i) {
      seeds.add(seed(i));
    }
    return seeds;
  }

  /** Response whose documents have a location, place ids and an is_closed flag. */
  private static SearchResponse poolResponse() throws Exception {
    StringBuilder hits = new StringBuilder();
    for (int i = 0; i < POOL_POINTS.length; ++i) {
      if (i > 0) {
        hits.append(',');
      }
      hits.append(
          """
          {"luceneDocId": %d, "fields": {
            "location": {"fieldValue": [{"latLngValue": {"latitude": %s, "longitude": %s}}]},
            "place_ids": {"fieldValue": [{"textValue": "doc%d_a"}, {"textValue": "doc%d_b"},
                                         {"textValue": "doc%d_c"}]},
            "is_closed": {"fieldValue": [{"booleanValue": true}]}}}
          """
              .formatted(i, POOL_POINTS[i][0], POOL_POINTS[i][1], i, i, i));
    }
    SearchResponse.Builder builder = SearchResponse.newBuilder();
    JsonFormat.parser().merge("{\"hits\": [" + hits + "]}", builder);
    return builder.build();
  }

  @Test
  public void testLearnsOneTemplateWithFieldSlots() throws Exception {
    TemplateWarmingQueryGenerator generator =
        new TemplateWarmingQueryGenerator(seeds(), new Random(1));
    assertEquals(1, generator.getNumTemplates());
    // is_closed is the same in every seed, so it is a constant, not a field to sample
    assertEquals(Set.of("location", "place_ids"), generator.fields());
    assertTrue(generator.slotKinds().get("constant") > 0);
  }

  @Test
  public void testGeneratesNewQueriesWithConsistentValues() throws Exception {
    List<SearchRequest> seeds = seeds();
    TemplateWarmingQueryGenerator generator =
        new TemplateWarmingQueryGenerator(seeds, new Random(1));
    generator.addPoolHits(poolResponse());
    assertEquals(POOL_POINTS.length, generator.getPoolSize());

    List<SearchRequest> generated = generator.generate(20);
    assertFalse(generated.isEmpty());
    Set<String> poolPlaceIds = new HashSet<>();
    for (int i = 0; i < POOL_POINTS.length; ++i) {
      poolPlaceIds.addAll(List.of("doc" + i + "_a", "doc" + i + "_b", "doc" + i + "_c"));
    }
    Set<SearchRequest> seedSet = new HashSet<>(seeds);
    for (SearchRequest request : generated) {
      assertFalse("generated a copy of a seed", seedSet.contains(request));
      // structure kept: same index, top hits, script and sort-relevant virtual field
      assertEquals("test_index", request.getIndexName());
      assertEquals(50, request.getTopHits());
      Script script = request.getVirtualFields(0).getScript();
      assertEquals("LAT + LON", script.getSource());

      List<Query> clauses =
          request.getQuery().getBooleanQuery().getClausesList().stream()
              .map(c -> c.getQuery())
              .toList();
      // constant filter keeps its real value, not the pool documents' value (true)
      assertFalse(clauses.get(0).getTermQuery().getBooleanValue());
      // the term set comes from one pool document
      List<String> terms = clauses.get(1).getTermInSetQuery().getTextTerms().getTermsList();
      assertEquals(2, terms.size());
      assertTrue(poolPlaceIds.containsAll(terms));
      assertEquals(terms.get(0).substring(0, 4), terms.get(1).substring(0, 4));
      // the geo centre comes from the pool, and the script params repeat it
      double lat = clauses.get(2).getGeoRadiusQuery().getCenter().getLatitude();
      double lon = clauses.get(2).getGeoRadiusQuery().getCenter().getLongitude();
      assertTrue(isPoolPoint(lat, lon));
      assertEquals(lat, script.getParamsMap().get("LAT").getDoubleValue(), 1e-9);
      assertEquals(lon, script.getParamsMap().get("LON").getDoubleValue(), 1e-9);
      // radius is not a field value, it keeps a real one
      assertEquals("10 mi", clauses.get(2).getGeoRadiusQuery().getRadius());
    }
  }

  @Test
  public void testWithoutPoolUsesSeedValues() throws Exception {
    TemplateWarmingQueryGenerator generator =
        new TemplateWarmingQueryGenerator(seeds(), new Random(1));
    List<SearchRequest> generated = generator.generate(20);
    assertFalse(generated.isEmpty());
    for (SearchRequest request : generated) {
      Query geo = request.getQuery().getBooleanQuery().getClauses(2).getQuery();
      assertNotEquals(0.0, geo.getGeoRadiusQuery().getCenter().getLatitude(), 1e-9);
    }
  }

  @Test
  public void testRareTemplateHasNoConstants() throws Exception {
    // fewer than MIN_INSTANCES seeds: constants can't be told apart, everything is variable
    TemplateWarmingQueryGenerator generator =
        new TemplateWarmingQueryGenerator(List.of(seed(0), seed(1)), new Random(1));
    assertEquals(0, (int) generator.slotKinds().getOrDefault("constant", 0));
    assertTrue(generator.fields().contains("is_closed"));
  }

  @Test
  public void testNoSeeds() {
    TemplateWarmingQueryGenerator generator =
        new TemplateWarmingQueryGenerator(List.of(), new Random(1));
    assertEquals(0, generator.getNumTemplates());
    assertTrue(generator.generate(10).isEmpty());
  }

  @Test
  public void testLibraryRoundTrip() throws Exception {
    TemplateWarmingQueryGenerator learned =
        new TemplateWarmingQueryGenerator(seeds(), new Random(1));
    TemplateWarmingQueryGenerator loaded =
        TemplateWarmingQueryGenerator.fromLibrary(learned.toLibrary(), new Random(1));
    assertEquals(learned.getNumTemplates(), loaded.getNumTemplates());
    assertEquals(learned.templateKeys(), loaded.templateKeys());
    assertEquals(learned.fields(), loaded.fields());
    assertEquals(learned.slotKinds(), loaded.slotKinds());
    // a loaded library generates the same kind of queries, with consistent values
    loaded.addPoolHits(poolResponse());
    List<SearchRequest> generated = loaded.generate(20);
    assertFalse(generated.isEmpty());
    for (SearchRequest request : generated) {
      Query geo = request.getQuery().getBooleanQuery().getClauses(2).getQuery();
      assertTrue(
          isPoolPoint(
              geo.getGeoRadiusQuery().getCenter().getLatitude(),
              geo.getGeoRadiusQuery().getCenter().getLongitude()));
      assertEquals(
          geo.getGeoRadiusQuery().getCenter().getLatitude(),
          request.getVirtualFields(0).getScript().getParamsMap().get("LAT").getDoubleValue(),
          1e-9);
    }
  }

  @Test
  public void testTemplateKeyMatchesLibrary() throws Exception {
    TemplateWarmingQueryGenerator learned =
        new TemplateWarmingQueryGenerator(seeds(), new Random(1));
    // any request with the seeds' structure matches, whatever its values
    assertTrue(learned.templateKeys().contains(TemplateWarmingQueryGenerator.templateKey(seed(3))));
    SearchRequest otherShape = seed(3).toBuilder().setTopHits(50).clearVirtualFields().build();
    assertFalse(
        learned.templateKeys().contains(TemplateWarmingQueryGenerator.templateKey(otherShape)));
  }

  @Test
  public void testMergeAddsNewTemplatesAndFadesOld() throws Exception {
    TemplateWarmingQueryGenerator old = new TemplateWarmingQueryGenerator(seeds(), new Random(1));
    List<SearchRequest> newShape = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      newShape.add(seed(i).toBuilder().clearVirtualFields().build());
    }
    TemplateWarmingQueryGenerator newer =
        new TemplateWarmingQueryGenerator(newShape, new Random(1));

    // weight of the old template: 10 seeds * 0.5 = 5 >= 1, kept
    TemplateWarmingQueryGenerator merged = old.merge(newer, 0.5, 1);
    assertEquals(2, merged.getNumTemplates());
    Set<String> keys = new HashSet<>(old.templateKeys());
    keys.addAll(newer.templateKeys());
    assertEquals(keys, merged.templateKeys());

    // old template decays below the minimum weight after repeated merges without it
    TemplateWarmingQueryGenerator faded = merged;
    for (int i = 0; i < 4; ++i) {
      faded = faded.merge(new TemplateWarmingQueryGenerator(newShape, new Random(1)), 0.5, 1);
    }
    assertEquals(newer.templateKeys(), faded.templateKeys());
  }

  @Test
  public void testMergeSameTemplateAddsWeight() throws Exception {
    TemplateWarmingQueryGenerator a = new TemplateWarmingQueryGenerator(seeds(), new Random(1));
    TemplateWarmingQueryGenerator b = new TemplateWarmingQueryGenerator(seeds(), new Random(2));
    TemplateWarmingQueryGenerator merged = a.merge(b, 0.5, 1);
    assertEquals(1, merged.getNumTemplates());
    assertEquals(a.templateKeys(), merged.templateKeys());
  }

  @Test
  public void testUnsupportedLibraryVersion() {
    org.junit.Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            TemplateWarmingQueryGenerator.fromLibrary(
                "{\"version\": 99, \"templates\": []}", new Random(1)));
  }

  private static boolean isPoolPoint(double lat, double lon) {
    for (double[] p : POOL_POINTS) {
      if (Math.abs(p[0] - lat) < 1e-9 && Math.abs(p[1] - lon) < 1e-9) {
        return true;
      }
    }
    return false;
  }
}
