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
package com.yelp.nrtsearch.server.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.Script;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class ScriptParamsUtilsTest {

  private Map<String, Script.ParamValue> encodeParams(Map<String, Object> map) {
    Script.Builder builder = Script.newBuilder();
    ScriptParamsUtils.encodeParams(builder, map);
    return builder.getParamsMap();
  }

  @Test(expected = NullPointerException.class)
  public void testEncodeNullBuilder() {
    Map<String, Object> map = new HashMap<>();
    ScriptParamsUtils.encodeParams(null, map);
  }

  @Test(expected = NullPointerException.class)
  public void testEncodeNullParams() {
    Script.Builder builder = Script.newBuilder();
    ScriptParamsUtils.encodeParams(builder, null);
  }

  @Test
  public void testEncodeEmptyParams() {
    Map<String, Script.ParamValue> paramsMap = encodeParams(Collections.emptyMap());
    assertEquals(paramsMap.size(), 0);
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

    Map<String, Script.ParamValue> paramsMap = encodeParams(map);
    assertEquals(7, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getTextValue());
    assertTrue(paramsMap.get("b").getBooleanValue());
    assertEquals(100, paramsMap.get("i").getIntValue());
    assertEquals(1001L, paramsMap.get("l").getLongValue());
    assertEquals(1.123F, paramsMap.get("f").getFloatValue(), Math.ulp(1.123F));
    assertEquals(2.345, paramsMap.get("d").getDoubleValue(), Math.ulp(2.345));
    assertEquals(Script.ParamNullValue.NULL_VALUE, paramsMap.get("n").getNullValue());
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

    Map<String, Script.ParamValue> paramsMap = encodeParams(map);
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getTextValue());
    assertTrue(paramsMap.get("b").getBooleanValue());

    Script.ParamListValue listValue = paramsMap.get("list").getListValue();
    assertEquals(5, listValue.getValuesCount());
    assertEquals(100, listValue.getValues(0).getIntValue());
    assertEquals(1001L, listValue.getValues(1).getLongValue());
    assertEquals(1.123F, listValue.getValues(2).getFloatValue(), Math.ulp(1.123F));
    assertEquals(2.345, listValue.getValues(3).getDoubleValue(), Math.ulp(2.345));
    assertEquals(Script.ParamNullValue.NULL_VALUE, listValue.getValues(4).getNullValue());
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

    Map<String, Script.ParamValue> paramsMap = encodeParams(map);
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getTextValue());
    assertTrue(paramsMap.get("b").getBooleanValue());

    Script.ParamListValue listValue = paramsMap.get("set").getListValue();
    assertEquals(5, listValue.getValuesCount());
    assertEquals(100, listValue.getValues(0).getIntValue());
    assertEquals(1001L, listValue.getValues(1).getLongValue());
    assertEquals(1.123F, listValue.getValues(2).getFloatValue(), Math.ulp(1.123F));
    assertEquals(2.345, listValue.getValues(3).getDoubleValue(), Math.ulp(2.345));
    assertEquals(Script.ParamNullValue.NULL_VALUE, listValue.getValues(4).getNullValue());
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

    Map<String, Script.ParamValue> paramsMap = encodeParams(map);
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getTextValue());
    assertTrue(paramsMap.get("b").getBooleanValue());

    Script.ParamListValue listValue = paramsMap.get("list").getListValue();
    assertEquals(3, listValue.getValuesCount());
    assertEquals(100, listValue.getValues(0).getIntValue());

    Script.ParamListValue listValue2 = listValue.getValues(1).getListValue();
    assertEquals(2, listValue2.getValuesCount());
    assertEquals(1001L, listValue2.getValues(0).getLongValue());
    assertEquals(1.123F, listValue2.getValues(1).getFloatValue(), Math.ulp(1.123F));

    Script.ParamStructValue struct2 = listValue.getValues(2).getStructValue();
    assertEquals(2, struct2.getFieldsCount());
    assertEquals(2.345, struct2.getFieldsOrThrow("d").getDoubleValue(), Math.ulp(2.345));
    assertEquals(Script.ParamNullValue.NULL_VALUE, struct2.getFieldsOrThrow("n").getNullValue());
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

    Map<String, Script.ParamValue> paramsMap = encodeParams(map);
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getTextValue());
    assertTrue(paramsMap.get("b").getBooleanValue());

    Script.ParamStructValue struct2 = paramsMap.get("struct").getStructValue();
    assertEquals(5, struct2.getFieldsCount());
    assertEquals(100, struct2.getFieldsOrThrow("i").getIntValue());
    assertEquals(1001L, struct2.getFieldsOrThrow("l").getLongValue());
    assertEquals(1.123F, struct2.getFieldsOrThrow("f").getFloatValue(), Math.ulp(1.123F));
    assertEquals(2.345, struct2.getFieldsOrThrow("d").getDoubleValue(), Math.ulp(2.345));
    assertEquals(Script.ParamNullValue.NULL_VALUE, struct2.getFieldsOrThrow("n").getNullValue());
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

    Map<String, Script.ParamValue> paramsMap = encodeParams(map);
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getTextValue());
    assertTrue(paramsMap.get("b").getBooleanValue());

    Script.ParamStructValue struct2 = paramsMap.get("struct").getStructValue();
    assertEquals(3, struct2.getFieldsCount());
    assertEquals(100, struct2.getFieldsOrThrow("i").getIntValue());

    Script.ParamListValue listValue = struct2.getFieldsOrThrow("list").getListValue();
    assertEquals(2, listValue.getValuesCount());
    assertEquals(1001L, listValue.getValues(0).getLongValue());
    assertEquals(1.123F, listValue.getValues(1).getFloatValue(), Math.ulp(1.123F));

    Script.ParamStructValue struct3 = struct2.getFieldsOrThrow("struct").getStructValue();
    assertEquals(2, struct3.getFieldsCount());
    assertEquals(2.345, struct3.getFieldsOrThrow("d").getDoubleValue(), Math.ulp(2.345));
    assertEquals(Script.ParamNullValue.NULL_VALUE, struct3.getFieldsOrThrow("n").getNullValue());
  }

  @Test
  public void testEncodeEmptyListItem() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    List<Object> list = new ArrayList<>();
    map.put("list", list);

    Map<String, Script.ParamValue> paramsMap = encodeParams(map);
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getTextValue());
    assertTrue(paramsMap.get("b").getBooleanValue());

    Script.ParamListValue listValue = paramsMap.get("list").getListValue();
    assertEquals(0, listValue.getValuesCount());
  }

  @Test
  public void testEncodeEmptyStructItem() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    Map<String, Object> map2 = new HashMap<>();
    map.put("struct", map2);

    Map<String, Script.ParamValue> paramsMap = encodeParams(map);
    assertEquals(3, paramsMap.size());
    assertEquals("test_str", paramsMap.get("s").getTextValue());
    assertTrue(paramsMap.get("b").getBooleanValue());

    Script.ParamStructValue struct2 = paramsMap.get("struct").getStructValue();
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

    encodeParams(map);
  }

  @Test(expected = NullPointerException.class)
  public void testEncodeNullMapKey() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    Map<String, Object> map2 = new HashMap<>();
    map2.put(null, 1L);
    map.put("struct", map2);

    encodeParams(map);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEncodeInvalidObject() {
    Map<String, Object> map = new HashMap<>();
    map.put("s", "test_str");
    map.put("b", true);
    map.put("invalid", new Object());

    encodeParams(map);
  }

  @Test(expected = NullPointerException.class)
  public void testDecodeNullStructCopy() {
    ScriptParamsUtils.decodeParams(null);
  }

  @Test
  public void testDecodeEmptyParams() {
    Map<String, Script.ParamValue> params = new HashMap<>();
    Map<String, Object> m = ScriptParamsUtils.decodeParams(params);

    assertEquals(0, m.size());
  }

  @Test
  public void testDecodeSimpleItems() {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("s", Script.ParamValue.newBuilder().setTextValue("test_str").build());
    params.put("b", Script.ParamValue.newBuilder().setBooleanValue(true).build());
    params.put("i", Script.ParamValue.newBuilder().setIntValue(100).build());
    params.put("l", Script.ParamValue.newBuilder().setLongValue(1001L).build());
    params.put("f", Script.ParamValue.newBuilder().setFloatValue(1.123F).build());
    params.put("d", Script.ParamValue.newBuilder().setDoubleValue(2.345).build());
    params.put(
        "n", Script.ParamValue.newBuilder().setNullValue(Script.ParamNullValue.NULL_VALUE).build());

    Map<String, Object> m = ScriptParamsUtils.decodeParams(params);
    assertEquals(7, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));
    assertEquals(100, m.get("i"));
    assertEquals(1001L, m.get("l"));
    assertEquals(1.123F, (Float) m.get("f"), Math.ulp(1.123F));
    assertEquals(2.345, (Double) m.get("d"), Math.ulp(2.345));
    assertNull(m.get("n"));
  }

  @Test
  public void testDecodeList() {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("s", Script.ParamValue.newBuilder().setTextValue("test_str").build());
    params.put("b", Script.ParamValue.newBuilder().setBooleanValue(true).build());
    params.put(
        "list",
        Script.ParamValue.newBuilder()
            .setListValue(
                Script.ParamListValue.newBuilder()
                    .addValues(Script.ParamValue.newBuilder().setIntValue(100).build())
                    .addValues(Script.ParamValue.newBuilder().setLongValue(1001L).build())
                    .addValues(Script.ParamValue.newBuilder().setFloatValue(1.123F).build())
                    .addValues(Script.ParamValue.newBuilder().setDoubleValue(2.345).build())
                    .addValues(
                        Script.ParamValue.newBuilder()
                            .setNullValue(Script.ParamNullValue.NULL_VALUE)
                            .build())
                    .build())
            .build());

    Map<String, Object> m = ScriptParamsUtils.decodeParams(params);
    assertEquals(3, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));

    List<?> list = (List<?>) m.get("list");
    assertEquals(5, list.size());
    assertEquals(100, list.get(0));
    assertEquals(1001L, list.get(1));
    assertEquals(1.123F, (Float) list.get(2), Math.ulp(1.123F));
    assertEquals(2.345, (Double) list.get(3), Math.ulp(2.345));
    assertNull(list.get(4));
  }

  @Test
  public void testDecodeComplexList() {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("s", Script.ParamValue.newBuilder().setTextValue("test_str").build());
    params.put("b", Script.ParamValue.newBuilder().setBooleanValue(true).build());
    params.put(
        "list",
        Script.ParamValue.newBuilder()
            .setListValue(
                Script.ParamListValue.newBuilder()
                    .addValues(Script.ParamValue.newBuilder().setIntValue(100).build())
                    .addValues(
                        Script.ParamValue.newBuilder()
                            .setListValue(
                                Script.ParamListValue.newBuilder()
                                    .addValues(
                                        Script.ParamValue.newBuilder().setLongValue(1001L).build())
                                    .addValues(
                                        Script.ParamValue.newBuilder()
                                            .setFloatValue(1.123F)
                                            .build())
                                    .build())
                            .build())
                    .addValues(
                        Script.ParamValue.newBuilder()
                            .setStructValue(
                                Script.ParamStructValue.newBuilder()
                                    .putFields(
                                        "d",
                                        Script.ParamValue.newBuilder()
                                            .setDoubleValue(2.345)
                                            .build())
                                    .putFields(
                                        "n",
                                        Script.ParamValue.newBuilder()
                                            .setNullValue(Script.ParamNullValue.NULL_VALUE)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build());

    Map<String, Object> m = ScriptParamsUtils.decodeParams(params);
    assertEquals(3, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));

    List<?> list = (List<?>) m.get("list");
    assertEquals(3, list.size());
    assertEquals(100, list.get(0));

    List<?> list2 = (List<?>) list.get(1);
    assertEquals(2, list2.size());
    assertEquals(1001L, list2.get(0));
    assertEquals(1.123F, (Float) list2.get(1), Math.ulp(1.123F));

    @SuppressWarnings("unchecked")
    Map<String, ?> m2 = (Map<String, ?>) list.get(2);
    assertEquals(2, m2.size());
    assertEquals(2.345, (Double) m2.get("d"), Math.ulp(2.345));
    assertNull(m2.get("n"));
  }

  @Test
  public void testDecodeStruct() {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("s", Script.ParamValue.newBuilder().setTextValue("test_str").build());
    params.put("b", Script.ParamValue.newBuilder().setBooleanValue(true).build());
    params.put(
        "struct",
        Script.ParamValue.newBuilder()
            .setStructValue(
                Script.ParamStructValue.newBuilder()
                    .putFields("i", Script.ParamValue.newBuilder().setIntValue(100).build())
                    .putFields("l", Script.ParamValue.newBuilder().setLongValue(1001L).build())
                    .putFields("f", Script.ParamValue.newBuilder().setFloatValue(1.123F).build())
                    .putFields("d", Script.ParamValue.newBuilder().setDoubleValue(2.345).build())
                    .putFields(
                        "n",
                        Script.ParamValue.newBuilder()
                            .setNullValue(Script.ParamNullValue.NULL_VALUE)
                            .build())
                    .build())
            .build());

    Map<String, Object> m = ScriptParamsUtils.decodeParams(params);
    assertEquals(3, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));

    @SuppressWarnings("unchecked")
    Map<String, ?> m2 = (Map<String, ?>) m.get("struct");
    assertEquals(5, m2.size());
    assertEquals(100, m2.get("i"));
    assertEquals(1001L, m2.get("l"));
    assertEquals(1.123F, (Float) m2.get("f"), Math.ulp(1.123F));
    assertEquals(2.345, (Double) m2.get("d"), Math.ulp(2.345));
    assertNull(m2.get("n"));
  }

  @Test
  public void testDecodeComplexStruct() {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("s", Script.ParamValue.newBuilder().setTextValue("test_str").build());
    params.put("b", Script.ParamValue.newBuilder().setBooleanValue(true).build());
    params.put(
        "struct",
        Script.ParamValue.newBuilder()
            .setStructValue(
                Script.ParamStructValue.newBuilder()
                    .putFields("i", Script.ParamValue.newBuilder().setIntValue(100).build())
                    .putFields(
                        "list",
                        Script.ParamValue.newBuilder()
                            .setListValue(
                                Script.ParamListValue.newBuilder()
                                    .addValues(
                                        Script.ParamValue.newBuilder().setLongValue(1001L).build())
                                    .addValues(
                                        Script.ParamValue.newBuilder()
                                            .setFloatValue(1.123F)
                                            .build())
                                    .build())
                            .build())
                    .putFields(
                        "struct",
                        Script.ParamValue.newBuilder()
                            .setStructValue(
                                Script.ParamStructValue.newBuilder()
                                    .putFields(
                                        "d",
                                        Script.ParamValue.newBuilder()
                                            .setDoubleValue(2.345)
                                            .build())
                                    .putFields(
                                        "n",
                                        Script.ParamValue.newBuilder()
                                            .setNullValue(Script.ParamNullValue.NULL_VALUE)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build());

    Map<String, Object> m = ScriptParamsUtils.decodeParams(params);
    assertEquals(3, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));

    @SuppressWarnings("unchecked")
    Map<String, ?> m2 = (Map<String, ?>) m.get("struct");
    assertEquals(3, m2.size());
    assertEquals(100, m2.get("i"));

    List<?> list = (List<?>) m2.get("list");
    assertEquals(2, list.size());
    assertEquals(1001L, list.get(0));
    assertEquals(1.123F, (Float) list.get(1), Math.ulp(1.123F));

    @SuppressWarnings("unchecked")
    Map<String, ?> m3 = (Map<String, ?>) m2.get("struct");
    assertEquals(2, m3.size());
    assertEquals(2.345, (Double) m3.get("d"), Math.ulp(2.345));
    assertNull(m3.get("n"));
  }

  @Test
  public void testDecodeEmptyListItem() {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("s", Script.ParamValue.newBuilder().setTextValue("test_str").build());
    params.put("b", Script.ParamValue.newBuilder().setBooleanValue(true).build());
    params.put(
        "list",
        Script.ParamValue.newBuilder()
            .setListValue(Script.ParamListValue.newBuilder().build())
            .build());

    Map<String, Object> m = ScriptParamsUtils.decodeParams(params);
    assertEquals(3, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));

    List<?> list = (List<?>) m.get("list");
    assertEquals(0, list.size());
  }

  @Test
  public void testDecodeEmptyStructItem() {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("s", Script.ParamValue.newBuilder().setTextValue("test_str").build());
    params.put("b", Script.ParamValue.newBuilder().setBooleanValue(true).build());
    params.put(
        "struct",
        Script.ParamValue.newBuilder()
            .setStructValue(Script.ParamStructValue.newBuilder().build())
            .build());

    Map<String, Object> m = ScriptParamsUtils.decodeParams(params);
    assertEquals(3, m.size());
    assertEquals("test_str", m.get("s"));
    assertTrue((Boolean) m.get("b"));

    @SuppressWarnings("unchecked")
    Map<String, ?> m2 = (Map<String, ?>) m.get("struct");
    assertEquals(0, m2.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDecodeUnsetValue() {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("s", Script.ParamValue.newBuilder().setTextValue("test_str").build());
    params.put("b", Script.ParamValue.newBuilder().setBooleanValue(true).build());
    params.put("struct", Script.ParamValue.newBuilder().build());
    ScriptParamsUtils.decodeParams(params);
  }

  @Test
  public void testEncodeFloatVector() {
    float[] vector = new float[] {1.0F, 2.0F, 3.0F};
    Script.ParamFloatVectorValue.Builder builder = Script.ParamFloatVectorValue.newBuilder();
    ScriptParamsUtils.encodeFloatVector(builder, vector);
    Script.ParamFloatVectorValue vectorValue = builder.build();
    assertEquals(3, vectorValue.getValuesCount());
    assertEquals(1.0F, vectorValue.getValues(0), Math.ulp(1.0F));
    assertEquals(2.0F, vectorValue.getValues(1), Math.ulp(2.0F));
    assertEquals(3.0F, vectorValue.getValues(2), Math.ulp(3.0F));
  }

  @Test
  public void testEncodeFloatVector_empty() {
    float[] vector = new float[0];
    Script.ParamFloatVectorValue.Builder builder = Script.ParamFloatVectorValue.newBuilder();
    ScriptParamsUtils.encodeFloatVector(builder, vector);
    Script.ParamFloatVectorValue vectorValue = builder.build();
    assertEquals(0, vectorValue.getValuesCount());
  }

  @Test
  public void testDecodeFloatVector() {
    Script.ParamFloatVectorValue vectorValue =
        Script.ParamFloatVectorValue.newBuilder()
            .addValues(1.0F)
            .addValues(2.0F)
            .addValues(3.0F)
            .build();
    float[] vector = ScriptParamsUtils.decodeFloatVector(vectorValue);
    assertEquals(3, vector.length);
    assertEquals(1.0F, vector[0], Math.ulp(1.0F));
    assertEquals(2.0F, vector[1], Math.ulp(2.0F));
    assertEquals(3.0F, vector[2], Math.ulp(3.0F));
  }

  @Test
  public void testDecodeFloatVector_empty() {
    Script.ParamFloatVectorValue vectorValue = Script.ParamFloatVectorValue.newBuilder().build();
    float[] vector = ScriptParamsUtils.decodeFloatVector(vectorValue);
    assertEquals(0, vector.length);
  }
}
