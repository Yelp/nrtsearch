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
package com.yelp.nrtsearch.server.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class StructJsonUtilsTest {
  @Test(expected = NullPointerException.class)
  public void testEncodeNullMap() {
    StructJsonUtils.convertMapToStruct(null);
  }

  @Test
  public void testEncodeEmptyMap() {
    Struct struct = StructJsonUtils.convertMapToStruct(Collections.emptyMap());
    assertEquals(0, struct.getFieldsCount());
  }

  @Test
  public void testEncodeSimpleItems() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    map.put("i", 100);
    map.put("l", 1001L);
    map.put("f", 1.123F);
    map.put("d", 2.345);
    map.put("n", null);

    Struct struct = StructJsonUtils.convertMapToStruct(map);
    Map<String, Value> paramsMap = struct.getFieldsMap();
    assertEquals(7, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getStringValue());
    assertTrue(paramsMap.get("b").getBoolValue());
    assertEquals(100, (int) paramsMap.get("i").getNumberValue());
    assertEquals(1001L, (long) paramsMap.get("l").getNumberValue());
    assertEquals(1.123F, (float) paramsMap.get("f").getNumberValue(), Math.ulp(1.123F));
    assertEquals(2.345, paramsMap.get("d").getNumberValue(), Math.ulp(2.345));
    assertEquals(NullValue.NULL_VALUE, paramsMap.get("n").getNullValue());
  }

  @Test
  public void testEncodeSimpleItemsLongAsString() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    map.put("i", 100);
    map.put("l", 1001L);
    map.put("f", 1.123F);
    map.put("d", 2.345);
    map.put("n", null);

    Struct struct = StructJsonUtils.convertMapToStruct(map, true);
    Map<String, Value> paramsMap = struct.getFieldsMap();
    assertEquals(7, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getStringValue());
    assertTrue(paramsMap.get("b").getBoolValue());
    assertEquals(100, (int) paramsMap.get("i").getNumberValue());
    assertEquals("1001", paramsMap.get("l").getStringValue());
    assertEquals(1.123F, (float) paramsMap.get("f").getNumberValue(), Math.ulp(1.123F));
    assertEquals(2.345, paramsMap.get("d").getNumberValue(), Math.ulp(2.345));
    assertEquals(NullValue.NULL_VALUE, paramsMap.get("n").getNullValue());
  }

  @Test
  public void testEncodeList() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    List<Object> list = new ArrayList<>();
    list.add(100);
    list.add(1001L);
    list.add(1.123F);
    list.add(2.345);
    list.add(null);
    map.put("list", list);

    Struct struct = StructJsonUtils.convertMapToStruct(map);
    Map<String, Value> paramsMap = struct.getFieldsMap();
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getStringValue());
    assertTrue(paramsMap.get("b").getBoolValue());

    ListValue listValue = paramsMap.get("list").getListValue();
    assertEquals(5, listValue.getValuesCount());
    assertEquals(100, (int) listValue.getValues(0).getNumberValue());
    assertEquals(1001L, (long) listValue.getValues(1).getNumberValue());
    assertEquals(1.123F, (float) listValue.getValues(2).getNumberValue(), Math.ulp(1.123F));
    assertEquals(2.345, listValue.getValues(3).getNumberValue(), Math.ulp(2.345));
    assertEquals(NullValue.NULL_VALUE, listValue.getValues(4).getNullValue());
  }

  @Test
  public void testEncodeListLongAsString() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    List<Object> list = new ArrayList<>();
    list.add(100);
    list.add(1001L);
    list.add(1.123F);
    list.add(2.345);
    list.add(null);
    map.put("list", list);

    Struct struct = StructJsonUtils.convertMapToStruct(map, true);
    Map<String, Value> paramsMap = struct.getFieldsMap();
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getStringValue());
    assertTrue(paramsMap.get("b").getBoolValue());

    ListValue listValue = paramsMap.get("list").getListValue();
    assertEquals(5, listValue.getValuesCount());
    assertEquals(100, (int) listValue.getValues(0).getNumberValue());
    assertEquals("1001", listValue.getValues(1).getStringValue());
    assertEquals(1.123F, (float) listValue.getValues(2).getNumberValue(), Math.ulp(1.123F));
    assertEquals(2.345, listValue.getValues(3).getNumberValue(), Math.ulp(2.345));
    assertEquals(NullValue.NULL_VALUE, listValue.getValues(4).getNullValue());
  }

  @Test
  public void testEncodeSet() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    Set<Object> set = new LinkedHashSet<>();
    set.add(100);
    set.add(1001L);
    set.add(1.123F);
    set.add(2.345);
    set.add(null);
    map.put("set", set);

    Struct struct = StructJsonUtils.convertMapToStruct(map);
    Map<String, Value> paramsMap = struct.getFieldsMap();
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getStringValue());
    assertTrue(paramsMap.get("b").getBoolValue());

    ListValue listValue = paramsMap.get("set").getListValue();
    assertEquals(5, listValue.getValuesCount());
    assertEquals(100, (int) listValue.getValues(0).getNumberValue());
    assertEquals(1001L, (long) listValue.getValues(1).getNumberValue());
    assertEquals(1.123F, (float) listValue.getValues(2).getNumberValue(), Math.ulp(1.123F));
    assertEquals(2.345, listValue.getValues(3).getNumberValue(), Math.ulp(2.345));
    assertEquals(NullValue.NULL_VALUE, listValue.getValues(4).getNullValue());
  }

  @Test
  public void testEncodeComplexList() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    List<Object> list = new ArrayList<>();
    list.add(100);
    List<Object> list2 = new ArrayList<>();
    list2.add(1001L);
    list2.add(1.123F);
    list.add(list2);
    Map<String, Object> map2 = new HashMap<>();
    map2.put("d", 2.345);
    map2.put("n", null);
    list.add(map2);
    map.put("list", list);

    Struct struct = StructJsonUtils.convertMapToStruct(map);
    Map<String, Value> paramsMap = struct.getFieldsMap();
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getStringValue());
    assertTrue(paramsMap.get("b").getBoolValue());

    ListValue listValue = paramsMap.get("list").getListValue();
    assertEquals(3, listValue.getValuesCount());
    assertEquals(100, (int) listValue.getValues(0).getNumberValue());

    ListValue listValue2 = listValue.getValues(1).getListValue();
    assertEquals(2, listValue2.getValuesCount());
    assertEquals(1001L, (long) listValue2.getValues(0).getNumberValue());
    assertEquals(1.123F, (float) listValue2.getValues(1).getNumberValue(), Math.ulp(1.123F));

    Struct struct2 = listValue.getValues(2).getStructValue();
    assertEquals(2, struct2.getFieldsCount());
    assertEquals(2.345, struct2.getFieldsOrThrow("d").getNumberValue(), Math.ulp(2.345));
    assertEquals(NullValue.NULL_VALUE, struct2.getFieldsOrThrow("n").getNullValue());
  }

  @Test
  public void testEncodeStruct() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    Map<String, Object> map2 = new HashMap<>();
    map2.put("i", 100);
    map2.put("l", 1001L);
    map2.put("f", 1.123F);
    map2.put("d", 2.345);
    map2.put("n", null);
    map.put("struct", map2);

    Struct struct = StructJsonUtils.convertMapToStruct(map);
    Map<String, Value> paramsMap = struct.getFieldsMap();
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getStringValue());
    assertTrue(paramsMap.get("b").getBoolValue());

    Struct struct2 = paramsMap.get("struct").getStructValue();
    assertEquals(5, struct2.getFieldsCount());
    assertEquals(100, (int) struct2.getFieldsOrThrow("i").getNumberValue());
    assertEquals(1001L, (long) struct2.getFieldsOrThrow("l").getNumberValue());
    assertEquals(1.123F, (float) struct2.getFieldsOrThrow("f").getNumberValue(), Math.ulp(1.123F));
    assertEquals(2.345, struct2.getFieldsOrThrow("d").getNumberValue(), Math.ulp(2.345));
    assertEquals(NullValue.NULL_VALUE, struct2.getFieldsOrThrow("n").getNullValue());
  }

  @Test
  public void testEncodeStructLongAsString() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    Map<String, Object> map2 = new HashMap<>();
    map2.put("i", 100);
    map2.put("l", 1001L);
    map2.put("f", 1.123F);
    map2.put("d", 2.345);
    map2.put("n", null);
    map.put("struct", map2);

    Struct struct = StructJsonUtils.convertMapToStruct(map, true);
    Map<String, Value> paramsMap = struct.getFieldsMap();
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getStringValue());
    assertTrue(paramsMap.get("b").getBoolValue());

    Struct struct2 = paramsMap.get("struct").getStructValue();
    assertEquals(5, struct2.getFieldsCount());
    assertEquals(100, (int) struct2.getFieldsOrThrow("i").getNumberValue());
    assertEquals("1001", struct2.getFieldsOrThrow("l").getStringValue());
    assertEquals(1.123F, (float) struct2.getFieldsOrThrow("f").getNumberValue(), Math.ulp(1.123F));
    assertEquals(2.345, struct2.getFieldsOrThrow("d").getNumberValue(), Math.ulp(2.345));
    assertEquals(NullValue.NULL_VALUE, struct2.getFieldsOrThrow("n").getNullValue());
  }

  @Test
  public void testEncodeComplexStruct() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    Map<String, Object> map2 = new HashMap<>();
    map2.put("i", 100);
    List<Object> list = new ArrayList<>();
    list.add(1001L);
    list.add(1.123F);
    map2.put("list", list);
    Map<String, Object> map3 = new HashMap<>();
    map3.put("d", 2.345);
    map3.put("n", null);
    map2.put("struct", map3);
    map.put("struct", map2);

    Struct struct = StructJsonUtils.convertMapToStruct(map);
    Map<String, Value> paramsMap = struct.getFieldsMap();
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getStringValue());
    assertTrue(paramsMap.get("b").getBoolValue());

    Struct struct2 = paramsMap.get("struct").getStructValue();
    assertEquals(3, struct2.getFieldsCount());
    assertEquals(100, (int) struct2.getFieldsOrThrow("i").getNumberValue());

    ListValue listValue = struct2.getFieldsOrThrow("list").getListValue();
    assertEquals(2, listValue.getValuesCount());
    assertEquals(1001L, (long) listValue.getValues(0).getNumberValue());
    assertEquals(1.123F, (float) listValue.getValues(1).getNumberValue(), Math.ulp(1.123F));

    Struct struct3 = struct2.getFieldsOrThrow("struct").getStructValue();
    assertEquals(2, struct3.getFieldsCount());
    assertEquals(2.345, struct3.getFieldsOrThrow("d").getNumberValue(), Math.ulp(2.345));
    assertEquals(NullValue.NULL_VALUE, struct3.getFieldsOrThrow("n").getNullValue());
  }

  @Test
  public void testEncodeEmptyListItem() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    List<Object> list = new ArrayList<>();
    map.put("list", list);

    Struct struct = StructJsonUtils.convertMapToStruct(map);
    Map<String, Value> paramsMap = struct.getFieldsMap();
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getStringValue());
    assertTrue(paramsMap.get("b").getBoolValue());

    ListValue listValue = paramsMap.get("list").getListValue();
    assertEquals(0, listValue.getValuesCount());
  }

  @Test
  public void testEncodeEmptyStructItem() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    Map<String, Object> map2 = new HashMap<>();
    map.put("struct", map2);

    Struct struct = StructJsonUtils.convertMapToStruct(map);
    Map<String, Value> paramsMap = struct.getFieldsMap();
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getStringValue());
    assertTrue(paramsMap.get("b").getBoolValue());

    Struct struct2 = paramsMap.get("struct").getStructValue();
    assertEquals(0, struct2.getFieldsCount());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEncodeInvalidMapKeyType() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    Map<Long, Object> map2 = new HashMap<>();
    map2.put(2L, 1L);
    map.put("struct", map2);

    StructJsonUtils.convertMapToStruct(map);
  }

  @Test(expected = NullPointerException.class)
  public void testEncodeNullMapKey() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    Map<String, Object> map2 = new HashMap<>();
    map2.put(null, 1L);
    map.put("struct", map2);

    StructJsonUtils.convertMapToStruct(map);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEncodeInvalidObject() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    map.put("invalid", new Object());

    StructJsonUtils.convertMapToStruct(map);
  }

  @Test(expected = NullPointerException.class)
  public void testDecodeNullStruct() {
    StructJsonUtils.convertStructToMap(null);
  }

  @Test
  public void testDecodeEmptyStruct() {
    Map<String, Object> map = StructJsonUtils.convertStructToMap(Struct.newBuilder().build());
    assertEquals(0, map.size());
  }

  @Test
  public void testDecodeSimpleItems() {
    Map<String, Value> params = new HashMap<>();
    params.put("s", Value.newBuilder().setStringValue("test_str").build());
    params.put("b", Value.newBuilder().setBoolValue(true).build());
    params.put("i", Value.newBuilder().setNumberValue(100).build());
    params.put("l", Value.newBuilder().setNumberValue(1001L).build());
    params.put("f", Value.newBuilder().setNumberValue(1.123F).build());
    params.put("d", Value.newBuilder().setNumberValue(2.345).build());
    params.put("n", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());

    Struct struct = Struct.newBuilder().putAllFields(params).build();
    Map<String, Object> m = StructJsonUtils.convertStructToMap(struct);
    assertEquals(7, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));
    assertEquals(100, ((Double) m.get("i")).intValue());
    assertEquals(1001L, ((Double) m.get("l")).longValue());
    assertEquals(1.123F, ((Double) m.get("f")).floatValue(), Math.ulp(1.123F));
    assertEquals(2.345, (Double) m.get("d"), Math.ulp(2.345));
    assertNull(m.get("n"));
  }

  @Test
  public void testDecodeList() {
    Map<String, Value> params = new HashMap<>();
    params.put("s", Value.newBuilder().setStringValue("test_str").build());
    params.put("b", Value.newBuilder().setBoolValue(true).build());
    params.put(
        "list",
        Value.newBuilder()
            .setListValue(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setNumberValue(100).build())
                    .addValues(Value.newBuilder().setNumberValue(1001L).build())
                    .addValues(Value.newBuilder().setNumberValue(1.123F).build())
                    .addValues(Value.newBuilder().setNumberValue(2.345).build())
                    .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                    .build())
            .build());

    Struct struct = Struct.newBuilder().putAllFields(params).build();
    Map<String, Object> m = StructJsonUtils.convertStructToMap(struct);
    assertEquals(3, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));

    List<?> list = (List<?>) m.get("list");
    assertEquals(5, list.size());
    assertEquals(100, ((Double) list.get(0)).intValue());
    assertEquals(1001L, ((Double) list.get(1)).longValue());
    assertEquals(1.123F, ((Double) list.get(2)).floatValue(), Math.ulp(1.123F));
    assertEquals(2.345, (Double) list.get(3), Math.ulp(2.345));
    assertNull(list.get(4));
  }

  @Test
  public void testDecodeComplexList() {
    Map<String, Value> params = new HashMap<>();
    params.put("s", Value.newBuilder().setStringValue("test_str").build());
    params.put("b", Value.newBuilder().setBoolValue(true).build());
    params.put(
        "list",
        Value.newBuilder()
            .setListValue(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setNumberValue(100).build())
                    .addValues(
                        Value.newBuilder()
                            .setListValue(
                                ListValue.newBuilder()
                                    .addValues(Value.newBuilder().setNumberValue(1001L).build())
                                    .addValues(Value.newBuilder().setNumberValue(1.123F).build())
                                    .build())
                            .build())
                    .addValues(
                        Value.newBuilder()
                            .setStructValue(
                                Struct.newBuilder()
                                    .putFields(
                                        "d", Value.newBuilder().setNumberValue(2.345).build())
                                    .putFields(
                                        "n",
                                        Value.newBuilder()
                                            .setNullValue(NullValue.NULL_VALUE)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build());

    Struct struct = Struct.newBuilder().putAllFields(params).build();
    Map<String, Object> m = StructJsonUtils.convertStructToMap(struct);
    assertEquals(3, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));

    List<?> list = (List<?>) m.get("list");
    assertEquals(3, list.size());
    assertEquals(100, ((Double) list.get(0)).intValue());

    List<?> list2 = (List<?>) list.get(1);
    assertEquals(2, list2.size());
    assertEquals(1001L, ((Double) list2.get(0)).longValue());
    assertEquals(1.123F, ((Double) list2.get(1)).floatValue(), Math.ulp(1.123F));

    @SuppressWarnings("unchecked")
    Map<String, ?> m2 = (Map<String, ?>) list.get(2);
    assertEquals(2, m2.size());
    assertEquals(2.345, (Double) m2.get("d"), Math.ulp(2.345));
    assertNull(m2.get("n"));
  }

  @Test
  public void testDecodeStruct() {
    Map<String, Value> params = new HashMap<>();
    params.put("s", Value.newBuilder().setStringValue("test_str").build());
    params.put("b", Value.newBuilder().setBoolValue(true).build());
    params.put(
        "struct",
        Value.newBuilder()
            .setStructValue(
                Struct.newBuilder()
                    .putFields("i", Value.newBuilder().setNumberValue(100).build())
                    .putFields("l", Value.newBuilder().setNumberValue(1001L).build())
                    .putFields("f", Value.newBuilder().setNumberValue(1.123F).build())
                    .putFields("d", Value.newBuilder().setNumberValue(2.345).build())
                    .putFields("n", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                    .build())
            .build());

    Struct struct = Struct.newBuilder().putAllFields(params).build();
    Map<String, Object> m = StructJsonUtils.convertStructToMap(struct);
    assertEquals(3, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));

    @SuppressWarnings("unchecked")
    Map<String, ?> m2 = (Map<String, ?>) m.get("struct");
    assertEquals(5, m2.size());
    assertEquals(100, ((Double) m2.get("i")).intValue());
    assertEquals(1001L, ((Double) m2.get("l")).longValue());
    assertEquals(1.123F, ((Double) m2.get("f")).floatValue(), Math.ulp(1.123F));
    assertEquals(2.345, (Double) m2.get("d"), Math.ulp(2.345));
    assertNull(m2.get("n"));
  }

  @Test
  public void testDecodeComplexStruct() {
    Map<String, Value> params = new HashMap<>();
    params.put("s", Value.newBuilder().setStringValue("test_str").build());
    params.put("b", Value.newBuilder().setBoolValue(true).build());
    params.put(
        "struct",
        Value.newBuilder()
            .setStructValue(
                Struct.newBuilder()
                    .putFields("i", Value.newBuilder().setNumberValue(100).build())
                    .putFields(
                        "list",
                        Value.newBuilder()
                            .setListValue(
                                ListValue.newBuilder()
                                    .addValues(Value.newBuilder().setNumberValue(1001L).build())
                                    .addValues(Value.newBuilder().setNumberValue(1.123F).build())
                                    .build())
                            .build())
                    .putFields(
                        "struct",
                        Value.newBuilder()
                            .setStructValue(
                                Struct.newBuilder()
                                    .putFields(
                                        "d", Value.newBuilder().setNumberValue(2.345).build())
                                    .putFields(
                                        "n",
                                        Value.newBuilder()
                                            .setNullValue(NullValue.NULL_VALUE)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build());

    Struct struct = Struct.newBuilder().putAllFields(params).build();
    Map<String, Object> m = StructJsonUtils.convertStructToMap(struct);
    assertEquals(3, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));

    @SuppressWarnings("unchecked")
    Map<String, ?> m2 = (Map<String, ?>) m.get("struct");
    assertEquals(3, m2.size());
    assertEquals(100, ((Double) m2.get("i")).intValue());

    List<?> list = (List<?>) m2.get("list");
    assertEquals(2, list.size());
    assertEquals(1001L, ((Double) list.get(0)).longValue());
    assertEquals(1.123F, ((Double) list.get(1)).floatValue(), Math.ulp(1.123F));

    @SuppressWarnings("unchecked")
    Map<String, ?> m3 = (Map<String, ?>) m2.get("struct");
    assertEquals(2, m3.size());
    assertEquals(2.345, (Double) m3.get("d"), Math.ulp(2.345));
    assertNull(m3.get("n"));
  }

  @Test
  public void testDecodeEmptyListItem() {
    Map<String, Value> params = new HashMap<>();
    params.put("s", Value.newBuilder().setStringValue("test_str").build());
    params.put("b", Value.newBuilder().setBoolValue(true).build());
    params.put("list", Value.newBuilder().setListValue(ListValue.newBuilder().build()).build());

    Struct struct = Struct.newBuilder().putAllFields(params).build();
    Map<String, Object> m = StructJsonUtils.convertStructToMap(struct);
    assertEquals(3, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));

    List<?> list = (List<?>) m.get("list");
    assertEquals(0, list.size());
  }

  @Test
  public void testDecodeEmptyStructItem() {
    Map<String, Value> params = new HashMap<>();
    params.put("s", Value.newBuilder().setStringValue("test_str").build());
    params.put("b", Value.newBuilder().setBoolValue(true).build());
    params.put("struct", Value.newBuilder().setStructValue(Struct.newBuilder().build()).build());

    Struct struct = Struct.newBuilder().putAllFields(params).build();
    Map<String, Object> m = StructJsonUtils.convertStructToMap(struct);
    assertEquals(3, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));

    @SuppressWarnings("unchecked")
    Map<String, ?> m2 = (Map<String, ?>) m.get("struct");
    assertEquals(0, m2.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDecodeUnsetValue() {
    Map<String, Value> params = new HashMap<>();
    params.put("s", Value.newBuilder().setStringValue("test_str").build());
    params.put("b", Value.newBuilder().setBoolValue(true).build());
    params.put("struct", Value.newBuilder().build());
    Struct struct = Struct.newBuilder().putAllFields(params).build();
    StructJsonUtils.convertStructToMap(struct);
  }
}
