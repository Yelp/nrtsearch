/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.state;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexGlobalState;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import java.io.IOException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StateUtilsTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testEnsureDirectoryCreatesDirs() {
    Path dirPath = Paths.get(folder.getRoot().getAbsolutePath(), "dir1", "dir2");
    assertFalse(dirPath.toFile().exists());

    StateUtils.ensureDirectory(dirPath);
    assertTrue(dirPath.toFile().exists());
    assertTrue(dirPath.toFile().isDirectory());
  }

  @Test
  public void testEnsureDirectoryExistsNoop() {
    Path dirPath = Paths.get(folder.getRoot().getAbsolutePath());
    assertTrue(dirPath.toFile().exists());

    StateUtils.ensureDirectory(dirPath);
    assertTrue(dirPath.toFile().exists());
    assertTrue(dirPath.toFile().isDirectory());
  }

  @Test
  public void testEnsureDirectoryFailsOnFile() throws IOException {
    Path filePath = Paths.get(folder.getRoot().getAbsolutePath(), "file");
    assertTrue(filePath.toFile().createNewFile());

    try {
      StateUtils.ensureDirectory(filePath);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("is not a directory"));
    }
  }

  @Test(expected = NullPointerException.class)
  public void testEnsureDirectoryNull() {
    StateUtils.ensureDirectory(null);
  }

  @Test
  public void testWriteNewStateFile() throws IOException {
    Path expectedStateFilePath =
        Paths.get(folder.getRoot().getAbsolutePath(), StateUtils.GLOBAL_STATE_FILE);
    assertFalse(expectedStateFilePath.toFile().exists());

    GlobalStateInfo globalStateInfo =
        GlobalStateInfo.newBuilder()
            .setGen(10)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(true).build())
            .putIndices(
                "test_index_2",
                IndexGlobalState.newBuilder().setId("test_id_2").setStarted(false).build())
            .build();

    StateUtils.writeStateToFile(
        globalStateInfo,
        Paths.get(folder.getRoot().getAbsolutePath()),
        StateUtils.GLOBAL_STATE_FILE);
    assertTrue(expectedStateFilePath.toFile().exists());

    GlobalStateInfo readState = StateUtils.readStateFromFile(expectedStateFilePath);
    assertEquals(globalStateInfo, readState);
  }

  @Test
  public void testReWriteStateFile() throws IOException {
    Path expectedStateFilePath =
        Paths.get(folder.getRoot().getAbsolutePath(), StateUtils.GLOBAL_STATE_FILE);
    assertFalse(expectedStateFilePath.toFile().exists());

    GlobalStateInfo globalStateInfo =
        GlobalStateInfo.newBuilder()
            .setGen(11)
            .putIndices(
                "test_index_3",
                IndexGlobalState.newBuilder().setId("test_id_3").setStarted(true).build())
            .putIndices(
                "test_index_4",
                IndexGlobalState.newBuilder().setId("test_id_4").setStarted(true).build())
            .build();

    StateUtils.writeStateToFile(
        globalStateInfo,
        Paths.get(folder.getRoot().getAbsolutePath()),
        StateUtils.GLOBAL_STATE_FILE);
    assertTrue(expectedStateFilePath.toFile().exists());

    GlobalStateInfo readState = StateUtils.readStateFromFile(expectedStateFilePath);
    assertEquals(globalStateInfo, readState);

    GlobalStateInfo globalStateInfo2 =
        GlobalStateInfo.newBuilder()
            .setGen(12)
            .putIndices(
                "test_index_5",
                IndexGlobalState.newBuilder().setId("test_id_5").setStarted(false).build())
            .putIndices(
                "test_index_6",
                IndexGlobalState.newBuilder().setId("test_id_6").setStarted(true).build())
            .putIndices(
                "test_index_7",
                IndexGlobalState.newBuilder().setId("test_id_7").setStarted(false).build())
            .build();

    StateUtils.writeStateToFile(
        globalStateInfo2,
        Paths.get(folder.getRoot().getAbsolutePath()),
        StateUtils.GLOBAL_STATE_FILE);
    assertTrue(expectedStateFilePath.toFile().exists());

    GlobalStateInfo readState2 = StateUtils.readStateFromFile(expectedStateFilePath);
    assertEquals(globalStateInfo2, readState2);
    assertNotEquals(readState, readState2);
  }

  @Test
  public void testWriteNullFile() throws IOException {
    try {
      StateUtils.writeStateToFile(
          null, Paths.get(folder.getRoot().getAbsolutePath()), StateUtils.GLOBAL_STATE_FILE);
      fail();
    } catch (NullPointerException ignore) {

    }
    try {
      StateUtils.writeStateToFile(
          GlobalStateInfo.newBuilder().build(), null, StateUtils.GLOBAL_STATE_FILE);
      fail();
    } catch (NullPointerException ignore) {

    }
    try {
      StateUtils.writeStateToFile(
          GlobalStateInfo.newBuilder().build(),
          Paths.get(folder.getRoot().getAbsolutePath()),
          null);
      fail();
    } catch (NullPointerException ignore) {

    }
  }

  @Test(expected = NoSuchFileException.class)
  public void testReadStateFileNotFound() throws IOException {
    Path expectedStateFilePath =
        Paths.get(folder.getRoot().getAbsolutePath(), StateUtils.GLOBAL_STATE_FILE);
    assertFalse(expectedStateFilePath.toFile().exists());
    StateUtils.readStateFromFile(expectedStateFilePath);
  }

  @Test(expected = NullPointerException.class)
  public void testReadNullFile() throws IOException {
    StateUtils.readStateFromFile(null);
  }

  @Test
  public void testWriteNewIndexStateFile() throws IOException {
    Path expectedStateFilePath =
        Paths.get(folder.getRoot().getAbsolutePath(), StateUtils.INDEX_STATE_FILE);
    assertFalse(expectedStateFilePath.toFile().exists());

    IndexStateInfo indexState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index")
            .setGen(5)
            .setCommitted(true)
            .setSettings(
                IndexSettings.newBuilder()
                    .setConcurrentMergeSchedulerMaxThreadCount(
                        Int32Value.newBuilder().setValue(10).build())
                    .setDirectory(StringValue.newBuilder().setValue("MMapDirectory").build())
                    .setIndexMergeSchedulerAutoThrottle(
                        BoolValue.newBuilder().setValue(true).build())
                    .setIndexSort(
                        SortFields.newBuilder()
                            .addSortedFields(
                                SortType.newBuilder()
                                    .setFieldName("field1")
                                    .setReverse(true)
                                    .build())
                            .addSortedFields(SortType.newBuilder().setFieldName("field2").build())
                            .build())
                    .build())
            .setLiveSettings(
                IndexLiveSettings.newBuilder()
                    .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(100).build())
                    .setIndexRamBufferSizeMB(DoubleValue.newBuilder().setValue(300.0).build())
                    .setMaxRefreshSec(DoubleValue.newBuilder().setValue(100.0).build())
                    .build())
            .build();

    StateUtils.writeIndexStateToFile(
        indexState, Paths.get(folder.getRoot().getAbsolutePath()), StateUtils.INDEX_STATE_FILE);
    assertTrue(expectedStateFilePath.toFile().exists());

    IndexStateInfo readState = StateUtils.readIndexStateFromFile(expectedStateFilePath);
    assertEquals(indexState, readState);
  }

  @Test
  public void testReWriteIndexStateFile() throws IOException {
    Path expectedStateFilePath =
        Paths.get(folder.getRoot().getAbsolutePath(), StateUtils.INDEX_STATE_FILE);
    assertFalse(expectedStateFilePath.toFile().exists());

    IndexStateInfo indexState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index_2")
            .setGen(5)
            .setCommitted(true)
            .setSettings(
                IndexSettings.newBuilder()
                    .setConcurrentMergeSchedulerMaxThreadCount(
                        Int32Value.newBuilder().setValue(15).build())
                    .setDirectory(StringValue.newBuilder().setValue("FSDirectory").build())
                    .setIndexMergeSchedulerAutoThrottle(
                        BoolValue.newBuilder().setValue(false).build())
                    .setIndexSort(
                        SortFields.newBuilder()
                            .addSortedFields(
                                SortType.newBuilder()
                                    .setFieldName("field1")
                                    .setReverse(false)
                                    .build())
                            .addSortedFields(SortType.newBuilder().setFieldName("field2").build())
                            .build())
                    .build())
            .setLiveSettings(
                IndexLiveSettings.newBuilder()
                    .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(200).build())
                    .setIndexRamBufferSizeMB(DoubleValue.newBuilder().setValue(100.0).build())
                    .setMaxRefreshSec(DoubleValue.newBuilder().setValue(50.0).build())
                    .build())
            .build();

    StateUtils.writeIndexStateToFile(
        indexState, Paths.get(folder.getRoot().getAbsolutePath()), StateUtils.INDEX_STATE_FILE);
    assertTrue(expectedStateFilePath.toFile().exists());

    IndexStateInfo readState = StateUtils.readIndexStateFromFile(expectedStateFilePath);
    assertEquals(indexState, readState);

    indexState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index_2")
            .setGen(6)
            .setCommitted(true)
            .setSettings(
                IndexSettings.newBuilder()
                    .setConcurrentMergeSchedulerMaxThreadCount(
                        Int32Value.newBuilder().setValue(16).build())
                    .setDirectory(StringValue.newBuilder().setValue("MMapDirectory").build())
                    .setIndexMergeSchedulerAutoThrottle(
                        BoolValue.newBuilder().setValue(true).build())
                    .setIndexSort(
                        SortFields.newBuilder()
                            .addSortedFields(
                                SortType.newBuilder()
                                    .setFieldName("field2")
                                    .setReverse(true)
                                    .build())
                            .build())
                    .build())
            .setLiveSettings(
                IndexLiveSettings.newBuilder()
                    .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(300).build())
                    .setIndexRamBufferSizeMB(DoubleValue.newBuilder().setValue(200.0).build())
                    .setMaxRefreshSec(DoubleValue.newBuilder().setValue(75.0).build())
                    .build())
            .build();

    StateUtils.writeIndexStateToFile(
        indexState, Paths.get(folder.getRoot().getAbsolutePath()), StateUtils.INDEX_STATE_FILE);
    assertTrue(expectedStateFilePath.toFile().exists());

    readState = StateUtils.readIndexStateFromFile(expectedStateFilePath);
    assertEquals(indexState, readState);
  }

  @Test
  public void testWriteNullIndexFile() throws IOException {
    try {
      StateUtils.writeIndexStateToFile(
          null, Paths.get(folder.getRoot().getAbsolutePath()), StateUtils.INDEX_STATE_FILE);
      fail();
    } catch (NullPointerException ignore) {

    }
    try {
      StateUtils.writeIndexStateToFile(
          IndexStateInfo.newBuilder().build(), null, StateUtils.INDEX_STATE_FILE);
      fail();
    } catch (NullPointerException ignore) {

    }
    try {
      StateUtils.writeIndexStateToFile(
          IndexStateInfo.newBuilder().build(), Paths.get(folder.getRoot().getAbsolutePath()), null);
      fail();
    } catch (NullPointerException ignore) {

    }
  }

  @Test(expected = NoSuchFileException.class)
  public void testReadIndexStateFileNotFound() throws IOException {
    Path expectedStateFilePath =
        Paths.get(folder.getRoot().getAbsolutePath(), StateUtils.INDEX_STATE_FILE);
    assertFalse(expectedStateFilePath.toFile().exists());
    StateUtils.readIndexStateFromFile(expectedStateFilePath);
  }

  @Test(expected = NullPointerException.class)
  public void testReadIndexNullFile() throws IOException {
    StateUtils.readIndexStateFromFile(null);
  }

  @Test
  public void testWriteToFile() throws IOException {
    StateUtils.writeToFile("test_data".getBytes(), folder.getRoot().toPath(), "test.txt");
    assertTrue(Paths.get(folder.getRoot().getAbsolutePath(), "test.txt").toFile().exists());
    byte[] fileBytes =
        Files.readAllBytes(Paths.get(folder.getRoot().getAbsolutePath(), "test.txt"));
    assertEquals("test_data", new String(fileBytes));
  }

  @Test
  public void testWriteToFile_rewrite() throws IOException {
    StateUtils.writeToFile("test_data".getBytes(), folder.getRoot().toPath(), "test.txt");
    StateUtils.writeToFile("test_data_2".getBytes(), folder.getRoot().toPath(), "test.txt");
    assertTrue(Paths.get(folder.getRoot().getAbsolutePath(), "test.txt").toFile().exists());
    byte[] fileBytes =
        Files.readAllBytes(Paths.get(folder.getRoot().getAbsolutePath(), "test.txt"));
    assertEquals("test_data_2", new String(fileBytes));
  }

  @Test
  public void testGlobalStateToUTF8() throws IOException {
    GlobalStateInfo globalStateInfo = getGlobalStateInfo();
    byte[] buffer = StateUtils.globalStateToUTF8(globalStateInfo);
    byte[] expected = getGlobalStateBytes();
    assertArrayEquals(expected, buffer);
  }

  @Test(expected = NullPointerException.class)
  public void testGlobalStateToUTF8_null() throws IOException {
    StateUtils.globalStateToUTF8(null);
  }

  @Test
  public void testGlobalStateFromUTF8() throws IOException {
    GlobalStateInfo globalStateInfo = getGlobalStateInfo();
    byte[] buffer = getGlobalStateBytes();
    GlobalStateInfo readState = StateUtils.globalStateFromUTF8(buffer);
    assertEquals(globalStateInfo, readState);
  }

  @Test
  public void testGlobalStateFromUTF8_invalid() {
    try {
      StateUtils.globalStateFromUTF8("invalid".getBytes());
      fail();
    } catch (IOException e) {
      assertEquals("Expect message object but got: \"invalid\"", e.getMessage());
    }
  }

  @Test
  public void testIndexStateToUTF8() throws IOException {
    IndexStateInfo indexStateInfo = getIndexStateInfo();
    byte[] buffer = StateUtils.indexStateToUTF8(indexStateInfo);
    byte[] expected = getIndexStateBytes();
    assertArrayEquals(expected, buffer);
  }

  @Test(expected = NullPointerException.class)
  public void testIndexStateToUTF8_null() throws IOException {
    StateUtils.indexStateToUTF8(null);
  }

  @Test
  public void testIndexStateFromUTF8() throws IOException {
    IndexStateInfo indexStateInfo = getIndexStateInfo();
    byte[] buffer = getIndexStateBytes();
    IndexStateInfo readState = StateUtils.indexStateFromUTF8(buffer);
    assertEquals(indexStateInfo, readState);
  }

  @Test
  public void testIndexStateFromUTF8_invalid() {
    try {
      StateUtils.indexStateFromUTF8("invalid".getBytes());
      fail();
    } catch (IOException e) {
      assertEquals("Expect message object but got: \"invalid\"", e.getMessage());
    }
  }

  @Test
  public void testGetValidatingUTF8Decoder() {
    CharsetDecoder decoder = StateUtils.getValidatingUTF8Decoder();
    assertNotNull(decoder);
    assertEquals(CodingErrorAction.REPORT, decoder.malformedInputAction());
    assertEquals(CodingErrorAction.REPORT, decoder.unmappableCharacterAction());
  }

  @Test
  public void testGetValidatingUTF8Encoder() {
    CharsetEncoder encoder = StateUtils.getValidatingUTF8Encoder();
    assertNotNull(encoder);
    assertEquals(CodingErrorAction.REPORT, encoder.malformedInputAction());
    assertEquals(CodingErrorAction.REPORT, encoder.unmappableCharacterAction());
  }

  private GlobalStateInfo getGlobalStateInfo() {
    return GlobalStateInfo.newBuilder()
        .setGen(10)
        .putIndices("index_1", IndexGlobalState.newBuilder().setId("id1").build())
        .putIndices("index_2", IndexGlobalState.newBuilder().setId("id2").setStarted(true).build())
        .build();
  }

  private byte[] getGlobalStateBytes() throws InvalidProtocolBufferException {
    return JsonFormat.printer().print(getGlobalStateInfo()).getBytes(StandardCharsets.UTF_8);
  }

  private IndexStateInfo getIndexStateInfo() {
    return IndexStateInfo.newBuilder()
        .setCommitted(true)
        .setGen(100)
        .setSettings(
            IndexSettings.newBuilder()
                .setNrtCachingDirectoryMaxMergeSizeMB(DoubleValue.newBuilder().setValue(10).build())
                .build())
        .setLiveSettings(
            IndexLiveSettings.newBuilder()
                .setIndexRamBufferSizeMB(DoubleValue.newBuilder().setValue(111).build())
                .build())
        .putFields("field1", Field.newBuilder().setSearch(true).build())
        .build();
  }

  private byte[] getIndexStateBytes() throws InvalidProtocolBufferException {
    return JsonFormat.printer().print(getIndexStateInfo()).getBytes(StandardCharsets.UTF_8);
  }
}
