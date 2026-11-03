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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.vectors.MetadataSchema.MetadataType;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class MetadataSchemaTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final IndexConfig config =
      new IndexConfig("bucket", "index", 4, IndexConfig.DistanceMetric.COSINE, List.of("text"), 0L);

  @Test
  public void testInferTypes() throws Exception {
    MetadataSchema schema = config.getMetadataSchema();
    Map<String, MetadataType> keys =
        schema.inferNewKeys(
            MAPPER.readTree(
                "{\"a\": \"x\", \"b\": 1.5, \"c\": true, \"d\": [\"x\", \"y\"], \"e\": [], "
                    + "\"text\": {\"nested\": 1}}"),
            config);
    assertEquals(
        Map.of(
            "a", MetadataType.STRING,
            "b", MetadataType.NUMBER,
            "c", MetadataType.BOOLEAN,
            "d", MetadataType.STRING),
        keys);
  }

  @Test
  public void testAddKeysCreatesFields() {
    MetadataSchema schema = config.getMetadataSchema();
    List<Field> fields =
        schema.addKeys(Map.of("a-b", MetadataType.STRING, "a_b", MetadataType.NUMBER));
    assertEquals(2, fields.size());
    String f1 = schema.getFieldName("a-b");
    String f2 = schema.getFieldName("a_b");
    assertTrue(f1.startsWith("m_a_b"));
    assertTrue(f2.startsWith("m_a_b"));
    assertTrue(!f1.equals(f2));
    for (Field field : fields) {
      assertTrue(field.getMultiValued());
      assertTrue(field.getSearch());
    }
    assertEquals(FieldType.ATOM, fieldFor(fields, f1).getType());
    assertEquals(FieldType.DOUBLE, fieldFor(fields, f2).getType());
    // adding again is a no-op
    assertTrue(schema.addKeys(Map.of("a-b", MetadataType.STRING)).isEmpty());
  }

  private static Field fieldFor(List<Field> fields, String name) {
    return fields.stream().filter(f -> f.getName().equals(name)).findFirst().orElseThrow();
  }

  @Test
  public void testTypeConflict() throws Exception {
    MetadataSchema schema = config.getMetadataSchema();
    schema.addKeys(Map.of("year", MetadataType.NUMBER));
    assertThrows(
        VectorsException.class,
        () -> schema.inferNewKeys(MAPPER.readTree("{\"year\": \"2020\"}"), config));
    assertTrue(schema.inferNewKeys(MAPPER.readTree("{\"year\": [1, 2]}"), config).isEmpty());
  }

  @Test
  public void testInvalidValues() {
    assertThrows(
        VectorsException.class, () -> MetadataSchema.inferType("k", MAPPER.readTree("{\"a\": 1}")));
    assertThrows(
        VectorsException.class, () -> MetadataSchema.inferType("k", MAPPER.readTree("[1, \"a\"]")));
    assertThrows(
        VectorsException.class, () -> MetadataSchema.inferType("k", MAPPER.readTree("[[1]]")));
    assertThrows(
        VectorsException.class, () -> MetadataSchema.inferType("k", MAPPER.readTree("null")));
    assertNull(uncheckedInfer("[]"));
  }

  private static MetadataType uncheckedInfer(String json) {
    try {
      return MetadataSchema.inferType("k", MAPPER.readTree(json));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFieldValues() throws Exception {
    assertEquals(List.of("2.0"), MetadataSchema.toFieldValues(MAPPER.readTree("2")));
    assertEquals(List.of("a", "b"), MetadataSchema.toFieldValues(MAPPER.readTree("[\"a\",\"b\"]")));
    assertEquals(List.of("true"), MetadataSchema.toFieldValues(MAPPER.readTree("true")));
  }
}
