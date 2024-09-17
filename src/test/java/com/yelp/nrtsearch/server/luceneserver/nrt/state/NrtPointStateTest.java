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
package com.yelp.nrtsearch.server.luceneserver.nrt.state;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.junit.Test;

public class NrtPointStateTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static final long version = 1;
  static final long gen = 3;
  static final byte[] infosBytes = new byte[] {1, 2, 3, 4, 5};
  static final long primaryGen = 5;
  static final Set<String> completedMergeFiles = Set.of("file1");
  static final String primaryId = "primaryId";
  static final FileMetaData fileMetaData =
      new FileMetaData(new byte[] {6, 7, 8}, new byte[] {0, 10, 11}, 10, 25);
  static final NrtFileMetaData nrtFileMetaData =
      new NrtFileMetaData(
          new byte[] {6, 7, 8}, new byte[] {0, 10, 11}, 10, 25, "primaryId2", "timeString");
  static final String expectedJson =
      "{\"files\":{\"file3\":{\"header\":\"BgcI\",\"footer\":\"AAoL\",\"length\":10,\"checksum\":25,\"primaryId\":\"primaryId2\",\"timeString\":\"timeString\"}},\"version\":1,\"gen\":3,\"infosBytes\":\"AQIDBAU=\",\"primaryGen\":5,\"completedMergeFiles\":[\"file1\"],\"primaryId\":\"primaryId\"}";

  public static NrtPointState getNrtPointState() {
    return new NrtPointState(getCopyState(), Map.of("file3", nrtFileMetaData), primaryId);
  }

  public static String getExpectedJson() {
    return expectedJson;
  }

  private static CopyState getCopyState() {
    return new CopyState(
        Map.of("file3", fileMetaData),
        version,
        gen,
        infosBytes,
        completedMergeFiles,
        primaryGen,
        null);
  }

  @Test
  public void testNrtPointState() {
    NrtPointState nrtPointState =
        new NrtPointState(getCopyState(), Map.of("file3", nrtFileMetaData), primaryId);
    assertNrtPointState(nrtPointState);
  }

  @Test
  public void testToCopyState() {
    NrtPointState nrtPointState =
        new NrtPointState(getCopyState(), Map.of("file3", nrtFileMetaData), primaryId);
    CopyState copyState = nrtPointState.toCopyState();
    assertEquals(copyState.version, version);
    assertEquals(copyState.gen, gen);
    assertArrayEquals(copyState.infosBytes, infosBytes);
    assertEquals(copyState.primaryGen, primaryGen);
    assertEquals(copyState.completedMergeFiles, completedMergeFiles);
    assertEquals(copyState.files.size(), 1);
    assertEquals(copyState.files.get("file3").length, 10);
    assertEquals(copyState.files.get("file3").checksum, 25);
    assertArrayEquals(copyState.files.get("file3").footer, new byte[] {0, 10, 11});
    assertArrayEquals(copyState.files.get("file3").header, new byte[] {6, 7, 8});
  }

  @Test
  public void testToJson() throws JsonProcessingException {
    NrtPointState nrtPointState =
        new NrtPointState(getCopyState(), Map.of("file3", nrtFileMetaData), primaryId);
    String json = OBJECT_MAPPER.writeValueAsString(nrtPointState);
    assertEquals(expectedJson, json);
  }

  @Test
  public void testFromJson() throws JsonProcessingException {
    NrtPointState nrtPointState = OBJECT_MAPPER.readValue(expectedJson, NrtPointState.class);
    assertNrtPointState(nrtPointState);
  }

  @Test
  public void testFromJson_unknownField() throws JsonProcessingException {
    String json =
        "{\"files\":{\"file3\":{\"header\":\"BgcI\",\"footer\":\"AAoL\",\"length\":10,\"checksum\":25,\"primaryId\":\"primaryId2\",\"timeString\":\"timeString\"}},\"version\":1,\"gen\":3,\"infosBytes\":\"AQIDBAU=\",\"primaryGen\":5,\"completedMergeFiles\":[\"file1\"],\"primaryId\":\"primaryId\",\"unknownField\":\"unknownValue\"}";
    NrtPointState nrtPointState = OBJECT_MAPPER.readValue(json, NrtPointState.class);
    assertNrtPointState(nrtPointState);
  }

  public static void assertNrtPointState(NrtPointState nrtPointState) {
    assertEquals(nrtPointState.version, version);
    assertEquals(nrtPointState.gen, gen);
    assertArrayEquals(nrtPointState.infosBytes, infosBytes);
    assertEquals(nrtPointState.primaryGen, primaryGen);
    assertEquals(nrtPointState.primaryId, primaryId);
    assertEquals(nrtPointState.completedMergeFiles, completedMergeFiles);
    assertEquals(nrtPointState.files.size(), 1);
    assertEquals(nrtPointState.files.get("file3").length, 10);
    assertEquals(nrtPointState.files.get("file3").checksum, 25);
    assertEquals(nrtPointState.files.get("file3").primaryId, "primaryId2");
    assertEquals(nrtPointState.files.get("file3").timeString, "timeString");
    assertArrayEquals(nrtPointState.files.get("file3").footer, new byte[] {0, 10, 11});
    assertArrayEquals(nrtPointState.files.get("file3").header, new byte[] {6, 7, 8});
  }
}
