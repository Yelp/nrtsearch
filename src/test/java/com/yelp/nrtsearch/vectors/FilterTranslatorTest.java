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
package com.yelp.nrtsearch.vectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.vectors.MetadataSchema.MetadataType;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class FilterTranslatorTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private IndexConfig config;

  @Before
  public void setUp() {
    config =
        new IndexConfig(
            "bucket", "index", 4, IndexConfig.DistanceMetric.COSINE, List.of("text"), 0L);
    config
        .getMetadataSchema()
        .addKeys(
            Map.of(
                "genre", MetadataType.STRING,
                "year", MetadataType.NUMBER,
                "active", MetadataType.BOOLEAN,
                "s3-path", MetadataType.STRING));
  }

  private Query translate(String json) throws Exception {
    JsonNode node = MAPPER.readTree(json);
    return FilterTranslator.translate(node, config);
  }

  private void assertInvalid(String json) {
    assertThrows(VectorsException.class, () -> translate(json));
  }

  @Test
  public void testImplicitEq() throws Exception {
    Query q = translate("{\"genre\": \"drama\"}");
    assertEquals("m_genre", q.getTermQuery().getField());
    assertEquals("drama", q.getTermQuery().getTextValue());
  }

  @Test
  public void testEqTypes() throws Exception {
    assertEquals(
        2020.0, translate("{\"year\": {\"$eq\": 2020}}").getTermQuery().getDoubleValue(), 0);
    assertTrue(translate("{\"active\": true}").getTermQuery().getBooleanValue());
  }

  @Test
  public void testEqTypeMismatchMatchesNothing() throws Exception {
    assertEquals(FilterTranslator.matchNone(), translate("{\"year\": \"2020\"}"));
  }

  @Test
  public void testUnknownKeyMatchesNothing() throws Exception {
    assertEquals(FilterTranslator.matchNone(), translate("{\"missing\": \"x\"}"));
    assertEquals(FilterTranslator.matchNone(), translate("{\"missing\": {\"$exists\": true}}"));
    // $exists false on a key nobody has matches everything
    assertEquals(
        FilterTranslator.not(FilterTranslator.matchNone()),
        translate("{\"missing\": {\"$exists\": false}}"));
  }

  @Test
  public void testSanitizedFieldName() throws Exception {
    Query q = translate("{\"s3-path\": {\"$startsWith\": \"/marketing/\"}}");
    assertEquals("m_s3_path", q.getPrefixQuery().getField());
    assertEquals("/marketing/", q.getPrefixQuery().getPrefix());
  }

  @Test
  public void testNe() throws Exception {
    Query q = translate("{\"genre\": {\"$ne\": \"drama\"}}");
    List<BooleanClause> clauses = q.getBooleanQuery().getClausesList();
    assertEquals(2, clauses.size());
    assertEquals(BooleanClause.Occur.MUST_NOT, clauses.get(1).getOccur());
    assertEquals("drama", clauses.get(1).getQuery().getTermQuery().getTextValue());
  }

  @Test
  public void testRanges() throws Exception {
    var gt = translate("{\"year\": {\"$gt\": 2019}}").getRangeQuery();
    assertEquals("2019.0", gt.getLower());
    assertTrue(gt.getLowerExclusive());
    var lte = translate("{\"year\": {\"$lte\": 2020}}").getRangeQuery();
    assertEquals("2020.0", lte.getUpper());
    assertTrue(!lte.getUpperExclusive());
    assertInvalid("{\"year\": {\"$gt\": \"2019\"}}");
  }

  @Test
  public void testMultipleOperatorsSameKey() throws Exception {
    Query q = translate("{\"year\": {\"$gte\": 10, \"$lte\": 50}}");
    List<BooleanClause> clauses = q.getBooleanQuery().getClausesList();
    assertEquals(2, clauses.size());
    clauses.forEach(c -> assertEquals(BooleanClause.Occur.FILTER, c.getOccur()));
  }

  @Test
  public void testIn() throws Exception {
    Query q = translate("{\"genre\": {\"$in\": [\"comedy\", \"documentary\"]}}");
    assertEquals(
        List.of("comedy", "documentary"), q.getTermInSetQuery().getTextTerms().getTermsList());
    Query nums = translate("{\"year\": {\"$in\": [1, 2]}}");
    assertEquals(List.of(1.0, 2.0), nums.getTermInSetQuery().getDoubleTerms().getTermsList());
    assertInvalid("{\"genre\": {\"$in\": []}}");
    assertInvalid("{\"genre\": {\"$in\": \"comedy\"}}");
  }

  @Test
  public void testNin() throws Exception {
    Query q = translate("{\"genre\": {\"$nin\": [\"comedy\"]}}");
    assertEquals(
        BooleanClause.Occur.MUST_NOT, q.getBooleanQuery().getClausesList().get(1).getOccur());
  }

  @Test
  public void testExists() throws Exception {
    assertEquals(
        "m_genre", translate("{\"genre\": {\"$exists\": true}}").getExistsQuery().getField());
    Query notExists = translate("{\"genre\": {\"$exists\": false}}");
    assertEquals(
        "m_genre",
        notExists.getBooleanQuery().getClauses(1).getQuery().getExistsQuery().getField());
    assertInvalid("{\"genre\": {\"$exists\": 1}}");
  }

  @Test
  public void testAndOr() throws Exception {
    Query and = translate("{\"$and\": [{\"genre\": \"drama\"}, {\"year\": {\"$gte\": 2020}}]}");
    assertEquals(2, and.getBooleanQuery().getClausesCount());
    assertEquals(BooleanClause.Occur.FILTER, and.getBooleanQuery().getClauses(0).getOccur());

    Query or = translate("{\"$or\": [{\"genre\": \"drama\"}, {\"year\": {\"$gte\": 2020}}]}");
    assertEquals(1, or.getBooleanQuery().getMinimumNumberShouldMatch());
    assertEquals(BooleanClause.Occur.SHOULD, or.getBooleanQuery().getClauses(0).getOccur());

    Query nested =
        translate(
            "{\"$or\": [{\"$and\": [{\"genre\": \"drama\"}, {\"active\": true}]}, {\"year\": 1}]}");
    assertEquals(
        2, nested.getBooleanQuery().getClauses(0).getQuery().getBooleanQuery().getClausesCount());
    assertInvalid("{\"$and\": []}");
    assertInvalid("{\"$or\": {\"genre\": \"drama\"}}");
  }

  @Test
  public void testNonFilterableKeyRejected() {
    assertInvalid("{\"text\": \"hello\"}");
  }

  @Test
  public void testInvalidFilters() {
    assertInvalid("[]");
    assertInvalid("{}");
    assertInvalid("{\"genre\": {}}");
    assertInvalid("{\"genre\": {\"$regex\": \"x\"}}");
    assertInvalid("{\"$not\": {\"genre\": \"x\"}}");
    assertInvalid("{\"genre\": {\"$eq\": [\"x\"]}}");
  }

  @Test
  public void testConstraintLimit() throws Exception {
    StringBuilder ok = new StringBuilder("{\"genre\": {\"$in\": [");
    for (int i = 0; i < 100; i++) {
      ok.append(i == 0 ? "" : ",").append("\"v").append(i).append("\"");
    }
    ok.append("]}}");
    translate(ok.toString());
    String tooMany = ok.toString().replace("]}}", ",\"extra\"]}}");
    assertInvalid(tooMany);
  }
}
