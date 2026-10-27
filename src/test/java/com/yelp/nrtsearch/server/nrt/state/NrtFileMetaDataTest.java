/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.nrt.state;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.junit.Test;

public class NrtFileMetaDataTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  final byte[] header = new byte[] {1, 2, 3, 4, 5};
  final byte[] footer = new byte[] {5, 4, 3, 2, 1};
  final long length = 10;
  final long checksum = 25;
  final String primaryId = "primaryId";
  final String timestamp = "timeString";

  final String expectedJson =
      "{\"header\":\"AQIDBAU=\",\"footer\":\"BQQDAgE=\",\"length\":10,\"checksum\":25,\"primaryId\":\"primaryId\",\"timeString\":\"timeString\"}";

  @Test
  public void testNrtFileMetaData() {
    NrtFileMetaData nrtFileMetaData =
        new NrtFileMetaData(header, footer, length, checksum, primaryId, timestamp);
    assertNrtFileMetaData(nrtFileMetaData);
  }

  @Test
  public void testFromFileMetaData() {
    FileMetaData fileMetaData = new FileMetaData(header, footer, length, checksum);
    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, primaryId, timestamp);
    assertNrtFileMetaData(nrtFileMetaData);
  }

  @Test
  public void testToJson() throws JsonProcessingException {
    NrtFileMetaData nrtFileMetaData =
        new NrtFileMetaData(header, footer, length, checksum, primaryId, timestamp);
    String json = OBJECT_MAPPER.writeValueAsString(nrtFileMetaData);
    assertEquals(expectedJson, json);
  }

  @Test
  public void testFromJson() throws JsonProcessingException {
    NrtFileMetaData nrtFileMetaData = OBJECT_MAPPER.readValue(expectedJson, NrtFileMetaData.class);
    assertNrtFileMetaData(nrtFileMetaData);
  }

  @Test
  public void testFromJson_unknownFields() throws JsonProcessingException {
    String json =
        "{\"header\":\"AQIDBAU=\",\"footer\":\"BQQDAgE=\",\"length\":10,\"checksum\":25,\"primaryId\":\"primaryId\",\"timeString\":\"timeString\",\"unknownField\":\"unknownValue\"}";
    NrtFileMetaData nrtFileMetaData = OBJECT_MAPPER.readValue(json, NrtFileMetaData.class);
    assertNrtFileMetaData(nrtFileMetaData);
  }

  @Test
  public void testToJson_withCompression() throws JsonProcessingException {
    NrtFileMetaData nrtFileMetaData =
        new NrtFileMetaData(header, footer, length, checksum, primaryId, timestamp);
    nrtFileMetaData.compressionType = "LZ4";
    nrtFileMetaData.compressedLength = 7L;
    String json = OBJECT_MAPPER.writeValueAsString(nrtFileMetaData);
    assertTrue(json.contains("\"compressionType\":\"LZ4\""));
    assertTrue(json.contains("\"compressedLength\":7"));
  }

  @Test
  public void testToJson_noCompressionFieldsWhenNull() throws JsonProcessingException {
    NrtFileMetaData nrtFileMetaData =
        new NrtFileMetaData(header, footer, length, checksum, primaryId, timestamp);
    String json = OBJECT_MAPPER.writeValueAsString(nrtFileMetaData);
    assertEquals(expectedJson, json);
  }

  @Test
  public void testFromJson_withCompression() throws JsonProcessingException {
    String json =
        "{\"header\":\"AQIDBAU=\",\"footer\":\"BQQDAgE=\",\"length\":10,\"checksum\":25,"
            + "\"primaryId\":\"primaryId\",\"timeString\":\"timeString\","
            + "\"compressionType\":\"LZ4\",\"compressedLength\":7}";
    NrtFileMetaData nrtFileMetaData = OBJECT_MAPPER.readValue(json, NrtFileMetaData.class);
    assertNrtFileMetaData(nrtFileMetaData);
    assertEquals("LZ4", nrtFileMetaData.compressionType);
    assertEquals(Long.valueOf(7), nrtFileMetaData.compressedLength);
  }

  @Test
  public void testFromJson_compressionFieldsNullWhenAbsent() throws JsonProcessingException {
    NrtFileMetaData nrtFileMetaData = OBJECT_MAPPER.readValue(expectedJson, NrtFileMetaData.class);
    assertNrtFileMetaData(nrtFileMetaData);
    assertNull(nrtFileMetaData.compressionType);
    assertNull(nrtFileMetaData.compressedLength);
  }

  @Test
  public void testEquals_withSameCompression() throws JsonProcessingException {
    NrtFileMetaData a = new NrtFileMetaData(header, footer, length, checksum, primaryId, timestamp);
    NrtFileMetaData b = new NrtFileMetaData(header, footer, length, checksum, primaryId, timestamp);
    a.compressionType = "LZ4";
    a.compressedLength = 7L;
    b.compressionType = "LZ4";
    b.compressedLength = 7L;
    assertEquals(a, b);
  }

  @Test
  public void testEquals_differentCompressionNotEqual() {
    NrtFileMetaData a = new NrtFileMetaData(header, footer, length, checksum, primaryId, timestamp);
    NrtFileMetaData b = new NrtFileMetaData(header, footer, length, checksum, primaryId, timestamp);
    a.compressionType = "LZ4";
    assertNotEquals(a, b);
  }

  private void assertNrtFileMetaData(NrtFileMetaData nrtFileMetaData) {
    assertArrayEquals(header, nrtFileMetaData.header);
    assertArrayEquals(footer, nrtFileMetaData.footer);
    assertEquals(length, nrtFileMetaData.length);
    assertEquals(checksum, nrtFileMetaData.checksum);
    assertEquals(primaryId, nrtFileMetaData.primaryId);
    assertEquals(timestamp, nrtFileMetaData.timeString);
  }
}
