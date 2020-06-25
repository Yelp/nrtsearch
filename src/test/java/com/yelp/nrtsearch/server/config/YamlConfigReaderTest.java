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
package com.yelp.nrtsearch.server.config;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class YamlConfigReaderTest {

  private YamlConfigReader getForConfig(String config) {
    return new YamlConfigReader(new ByteArrayInputStream(config.getBytes()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyConfig() {
    YamlConfigReader reader = getForConfig("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testArrayConfig() {
    String config = String.join("\n", "- val1", "- val2", "- val3");
    YamlConfigReader reader = getForConfig(config);
  }

  @Test
  public void testHash() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "key1: val1", "key2: 'val2'", "key3: value 3");
    YamlConfigReader reader = getForConfig(config);
    assertEquals("val1", reader.getString("key1"));
    assertEquals("val2", reader.getString("key2"));
    assertEquals("value 3", reader.getString("key3"));
  }

  @Test
  public void testNestedHash() throws ConfigKeyNotFoundException {
    String config =
        String.join(
            "\n",
            "l11:",
            "  l21:",
            "    l31: val1",
            "    l32:",
            "      l41: val2",
            "      l42: val3",
            "  l22:",
            "    l33: val4",
            "    l34: val5",
            "l12: val6");
    YamlConfigReader reader = getForConfig(config);
    assertEquals("val1", reader.getString("l11.l21.l31"));
    assertEquals("val2", reader.getString("l11.l21.l32.l41"));
    assertEquals("val3", reader.getString("l11.l21.l32.l42"));
    assertEquals("val4", reader.getString("l11.l22.l33"));
    assertEquals("val5", reader.getString("l11.l22.l34"));
    assertEquals("val6", reader.getString("l12"));
  }

  @Test(expected = ConfigKeyNotFoundException.class)
  public void testKeyNotFound() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "  l21: val1");
    YamlConfigReader reader = getForConfig(config);
    reader.getString("l11.not_exist");
  }

  @Test(expected = ConfigKeyNotFoundException.class)
  public void testRootNotFound() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "  l21: val1");
    YamlConfigReader reader = getForConfig(config);
    reader.getString("not_exist");
  }

  @Test(expected = ConfigKeyNotFoundException.class)
  public void testPathNotFound() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "  l21: val1");
    YamlConfigReader reader = getForConfig(config);
    reader.getString("l11.not_exist.l31");
  }

  @Test(expected = ConfigReadException.class)
  public void testValueInPath() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "  l21: val1");
    YamlConfigReader reader = getForConfig(config);
    reader.getString("l11.l21.l31");
  }

  @Test(expected = ConfigReadException.class)
  public void testArrayInPath() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "  l21:", "    - val1", "    - val2");
    YamlConfigReader reader = getForConfig(config);
    reader.getString("l11.l21.l31");
  }

  @Test
  public void testArray() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "l12:", "  - val1", "  - val2");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Arrays.asList("val1", "val2"), reader.getStringList("l12"));
  }

  @Test
  public void testArrayInNestedHash() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "  l21:", "    - val1", "    - val2");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Arrays.asList("val1", "val2"), reader.getStringList("l11.l21"));
  }

  @Test(expected = ConfigReadException.class)
  public void testHashNotArray() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "  l21:", "    - val1", "    - val2");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Arrays.asList("val1", "val2"), reader.getStringList("l11"));
  }

  @Test(expected = ConfigReadException.class)
  public void testValueNotArray() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "  l21: val1");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Arrays.asList("val1", "val2"), reader.getStringList("l11.l21"));
  }

  @Test
  public void testEmptyPath() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11: val1", "\"\": val2");
    YamlConfigReader reader = getForConfig(config);
    assertEquals("val2", reader.getString(""));
  }

  @Test
  public void testEmptyStrInPath() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "  \"\":", "    l31: val1");
    YamlConfigReader reader = getForConfig(config);
    assertEquals("val1", reader.getString("l11..l31"));
  }

  @Test
  public void testEmptyStrEndOfPath() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "  l21:", "    \"\": val1");
    YamlConfigReader reader = getForConfig(config);
    assertEquals("val1", reader.getString("l11.l21."));
  }

  @Test
  public void testReadStr() throws ConfigKeyNotFoundException {
    String config =
        String.join("\n", "key1: 10", "key2: some value", "key3: 'val1'", "key4: 100.1");
    YamlConfigReader reader = getForConfig(config);
    assertEquals("10", reader.getString("key1"));
    assertEquals("some value", reader.getString("key2"));
    assertEquals("val1", reader.getString("key3"));
    assertEquals("100.1", reader.getString("key4"));
  }

  @Test
  public void testReadStrList() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  - 10", "  - some value", "  - 'val1'", "  - 100.1");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Arrays.asList("10", "some value", "val1", "100.1"), reader.getStringList("l1"));
  }

  @Test(expected = ConfigReadException.class)
  public void testInvalidStr() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "key1:", "  key2: val1");
    YamlConfigReader reader = getForConfig(config);
    reader.getString("key1");
  }

  @Test(expected = ConfigReadException.class)
  public void testInvalidStrList() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  - 10", "  - false", "  - l2: val1");
    YamlConfigReader reader = getForConfig(config);
    reader.getStringList("l1");
  }

  @Test
  public void testDefaultStr() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    assertEquals("default_value", reader.getString("key2", "default_value"));
  }

  @Test
  public void testDefaultStrList() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(
        Arrays.asList("val1", "val2"), reader.getStringList("key2", Arrays.asList("val1", "val2")));
  }

  @Test(expected = ConfigReadException.class)
  public void testDefaultStrException() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getString("key1.key2", "default_value");
  }

  @Test(expected = ConfigReadException.class)
  public void testDefaultStrListException() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getStringList("key1.key2", Arrays.asList("val1", "val2"));
  }

  @Test
  public void testReadBool() throws ConfigKeyNotFoundException {
    String config =
        String.join(
            "\n",
            "key1: true",
            "key2: false",
            "key3: 'true'",
            "key4: 'TRUE'",
            "key5: 'false'",
            "key6: 'not_true'");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Boolean.TRUE, reader.getBoolean("key1"));
    assertEquals(Boolean.FALSE, reader.getBoolean("key2"));
    assertEquals(Boolean.TRUE, reader.getBoolean("key3"));
    assertEquals(Boolean.TRUE, reader.getBoolean("key4"));
    assertEquals(Boolean.FALSE, reader.getBoolean("key5"));
    assertEquals(Boolean.FALSE, reader.getBoolean("key6"));
  }

  @Test
  public void testReadBoolList() throws ConfigKeyNotFoundException {
    String config =
        String.join(
            "\n",
            "l1:",
            "  - true",
            "  - false",
            "  - 'true'",
            "  - 'TRUE'",
            "  - 'false'",
            "  - 'not_true'");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(
        Arrays.asList(
            Boolean.TRUE, Boolean.FALSE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE),
        reader.getBooleanList("l1"));
  }

  @Test(expected = ConfigReadException.class)
  public void testInvalidBool() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getBoolean("key1");
  }

  @Test(expected = ConfigReadException.class)
  public void testInvalidBoolList() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  - true", "  - false", "  - 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getBooleanList("l1");
  }

  @Test
  public void testDefaultBool() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Boolean.FALSE, reader.getBoolean("key2", Boolean.FALSE));
  }

  @Test
  public void testDefaultBoolList() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(
        Arrays.asList(Boolean.FALSE, Boolean.TRUE),
        reader.getBooleanList("key2", Arrays.asList(Boolean.FALSE, Boolean.TRUE)));
  }

  @Test(expected = ConfigReadException.class)
  public void testDefaultBoolException() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getBoolean("key1.key2", Boolean.FALSE);
  }

  @Test(expected = ConfigReadException.class)
  public void testDefaultBoolListException() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getBooleanList("key1.key2", Arrays.asList(Boolean.FALSE, Boolean.TRUE));
  }

  @Test
  public void testReadInt() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "key1: 10", "key2: '100'");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Integer.valueOf(10), reader.getInteger("key1"));
    assertEquals(Integer.valueOf(100), reader.getInteger("key2"));
  }

  @Test
  public void testReadIntList() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  - 10", "  - 15.1", "  - '100'");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Arrays.asList(10, 15, 100), reader.getIntegerList("l1"));
  }

  @Test(expected = ConfigReadException.class)
  public void testInvalidInt() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "key1: true");
    YamlConfigReader reader = getForConfig(config);
    reader.getInteger("key1");
  }

  @Test(expected = ConfigReadException.class)
  public void testInvalidIntList() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  - 10", "  - false", "  - 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getIntegerList("l1");
  }

  @Test
  public void testDefaultInt() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Integer.valueOf(10), reader.getInteger("key2", 10));
  }

  @Test
  public void testDefaultIntList() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Arrays.asList(10, 1000), reader.getIntegerList("key2", Arrays.asList(10, 1000)));
  }

  @Test(expected = ConfigReadException.class)
  public void testDefaultIntException() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getInteger("key1.key2", 10);
  }

  @Test(expected = ConfigReadException.class)
  public void testDefaultIntListException() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getIntegerList("key1.key2", Arrays.asList(10, 1000));
  }

  @Test
  public void testReadLong() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "key1: 10", "key2: '100'");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Long.valueOf(10), reader.getLong("key1"));
    assertEquals(Long.valueOf(100), reader.getLong("key2"));
  }

  @Test
  public void testReadLongList() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  - 10", "  - 15.1", "  - '100'");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Arrays.asList(10L, 15L, 100L), reader.getLongList("l1"));
  }

  @Test(expected = ConfigReadException.class)
  public void testInvalidLong() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "key1: true");
    YamlConfigReader reader = getForConfig(config);
    reader.getLong("key1");
  }

  @Test(expected = ConfigReadException.class)
  public void testInvalidLongList() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  - 10", "  - false", "  - 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getLongList("l1");
  }

  @Test
  public void testDefaultLong() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Long.valueOf(10), reader.getLong("key2", 10L));
  }

  @Test
  public void testDefaultLongList() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Arrays.asList(10L, 1000L), reader.getLongList("key2", Arrays.asList(10L, 1000L)));
  }

  @Test(expected = ConfigReadException.class)
  public void testDefaultLongException() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getLong("key1.key2", 10L);
  }

  @Test(expected = ConfigReadException.class)
  public void testDefaultLongListException() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getLongList("key1.key2", Arrays.asList(10L, 1000L));
  }

  @Test
  public void testReadFloat() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "key1: 10.2", "key2: '100.5'");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(10.2F, reader.getFloat("key1"), Math.ulp(10.2F));
    assertEquals(100.5F, reader.getFloat("key2"), Math.ulp(10.2F));
  }

  @Test
  public void testReadFloatList() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  - 10", "  - 15.1", "  - '100.5'");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Arrays.asList(10.0F, 15.1F, 100.5F), reader.getFloatList("l1"));
  }

  @Test(expected = ConfigReadException.class)
  public void testInvalidFloat() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "key1: true");
    YamlConfigReader reader = getForConfig(config);
    reader.getFloat("key1");
  }

  @Test(expected = ConfigReadException.class)
  public void testInvalidFloatList() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  - 10", "  - false", "  - 100.5");
    YamlConfigReader reader = getForConfig(config);
    reader.getFloatList("l1");
  }

  @Test
  public void testDefaultFloat() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(10.5F, reader.getFloat("key2", 10.5F), Math.ulp(10.5F));
  }

  @Test
  public void testDefaultFloatList() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(
        Arrays.asList(10.5F, 1000F), reader.getFloatList("key2", Arrays.asList(10.5F, 1000F)));
  }

  @Test(expected = ConfigReadException.class)
  public void testDefaultFloatException() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getFloat("key1.key2", 10.1F);
  }

  @Test(expected = ConfigReadException.class)
  public void testDefaultFloatListException() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getFloatList("key1.key2", Arrays.asList(10.1F, 1000F));
  }

  @Test
  public void testReadDouble() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "key1: 10.2", "key2: '100.5'");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(10.2, reader.getDouble("key1"), Math.ulp(10.2));
    assertEquals(100.5, reader.getDouble("key2"), Math.ulp(10.2));
  }

  @Test
  public void testReadDoubleList() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  - 10", "  - 15.1", "  - '100.5'");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Arrays.asList(10.0, 15.1, 100.5), reader.getDoubleList("l1"));
  }

  @Test(expected = ConfigReadException.class)
  public void testInvalidDouble() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "key1: true");
    YamlConfigReader reader = getForConfig(config);
    reader.getDouble("key1");
  }

  @Test(expected = ConfigReadException.class)
  public void testInvalidDoubleList() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  - 10", "  - false", "  - 100.5");
    YamlConfigReader reader = getForConfig(config);
    reader.getDoubleList("l1");
  }

  @Test
  public void testDefaultDouble() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(10.5, reader.getDouble("key2", 10.5), Math.ulp(10.5));
  }

  @Test
  public void testDefaultDoubleList() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(
        Arrays.asList(10.5, 1000D), reader.getDoubleList("key2", Arrays.asList(10.5, 1000D)));
  }

  @Test(expected = ConfigReadException.class)
  public void testDefaultDoubleException() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getDouble("key1.key2", 10.1);
  }

  @Test(expected = ConfigReadException.class)
  public void testDefaultDoubleListException() {
    String config = String.join("\n", "key1: 100");
    YamlConfigReader reader = getForConfig(config);
    reader.getDoubleList("key1.key2", Arrays.asList(10.1, 1000D));
  }

  @Test
  public void testGetKeys() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  key1: val1", "  key2: val2", "  key3: val3");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Set.of("key1", "key2", "key3"), Set.copyOf(reader.getKeys("l1")));
  }

  @Test
  public void testGetKeysNestedHash() throws ConfigKeyNotFoundException {
    String config =
        String.join(
            "\n",
            "l11:",
            "  l21:",
            "    l31:",
            "      key1: val1",
            "      key2: val2",
            "    key3: val3",
            "  key4: val4");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Set.of("l21", "key4"), Set.copyOf(reader.getKeys("l11")));
    assertEquals(Set.of("l31", "key3"), Set.copyOf(reader.getKeys("l11.l21")));
    assertEquals(Set.of("key1", "key2"), Set.copyOf(reader.getKeys("l11.l21.l31")));
  }

  @Test(expected = ConfigReadException.class)
  public void testGetKeysInvalid() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "  l21: val1");
    YamlConfigReader reader = getForConfig(config);
    reader.getKeys("l11.l21.l31");
  }

  @Test(expected = ConfigKeyNotFoundException.class)
  public void testGetKeysNotFound() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l11:", "  l21: val1");
    YamlConfigReader reader = getForConfig(config);
    reader.getKeys("l11.l22");
  }

  @Test
  public void testGetKeysOrEmpty() {
    String config = String.join("\n", "l1:", "  key1: val1", "  key2: val2", "  key3: val3");
    YamlConfigReader reader = getForConfig(config);
    assertEquals(Set.of("key1", "key2", "key3"), Set.copyOf(reader.getKeysOrEmpty("l1")));
    assertEquals(Collections.emptyList(), reader.getKeysOrEmpty("l1.l2"));
  }

  @Test(expected = ConfigReadException.class)
  public void testGetKeysInvalidType() throws ConfigKeyNotFoundException {
    String config = String.join("\n", "l1:", "  key1: val1", "  ~: val2", "  key3: val3");
    YamlConfigReader reader = getForConfig(config);
    reader.getKeys("l1");
  }
}
