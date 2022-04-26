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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.state.PersistentGlobalState.IndexInfo;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
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

    Map<String, IndexInfo> testIndices = new HashMap<>();
    testIndices.put("test_index", new IndexInfo("test_id_1"));
    testIndices.put("test_index_2", new IndexInfo("test_id_2"));
    PersistentGlobalState persistentGlobalState = new PersistentGlobalState(testIndices);

    StateUtils.writeStateToFile(
        persistentGlobalState,
        Paths.get(folder.getRoot().getAbsolutePath()),
        StateUtils.GLOBAL_STATE_FILE);
    assertTrue(expectedStateFilePath.toFile().exists());

    PersistentGlobalState readState = StateUtils.readStateFromFile(expectedStateFilePath);
    assertEquals(persistentGlobalState, readState);
  }

  @Test
  public void testReWriteStateFile() throws IOException {
    Path expectedStateFilePath =
        Paths.get(folder.getRoot().getAbsolutePath(), StateUtils.GLOBAL_STATE_FILE);
    assertFalse(expectedStateFilePath.toFile().exists());

    Map<String, IndexInfo> testIndices = new HashMap<>();
    testIndices.put("test_index_3", new IndexInfo("test_id_3"));
    testIndices.put("test_index_4", new IndexInfo("test_id_4"));
    PersistentGlobalState persistentGlobalState = new PersistentGlobalState(testIndices);

    StateUtils.writeStateToFile(
        persistentGlobalState,
        Paths.get(folder.getRoot().getAbsolutePath()),
        StateUtils.GLOBAL_STATE_FILE);
    assertTrue(expectedStateFilePath.toFile().exists());

    PersistentGlobalState readState = StateUtils.readStateFromFile(expectedStateFilePath);
    assertEquals(persistentGlobalState, readState);

    testIndices = new HashMap<>();
    testIndices.put("test_index_5", new IndexInfo("test_id_5"));
    testIndices.put("test_index_6", new IndexInfo("test_id_6"));
    testIndices.put("test_index_7", new IndexInfo("test_id_7"));
    PersistentGlobalState persistentGlobalState2 = new PersistentGlobalState(testIndices);

    StateUtils.writeStateToFile(
        persistentGlobalState2,
        Paths.get(folder.getRoot().getAbsolutePath()),
        StateUtils.GLOBAL_STATE_FILE);
    assertTrue(expectedStateFilePath.toFile().exists());

    PersistentGlobalState readState2 = StateUtils.readStateFromFile(expectedStateFilePath);
    assertEquals(persistentGlobalState2, readState2);
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
      StateUtils.writeStateToFile(new PersistentGlobalState(), null, StateUtils.GLOBAL_STATE_FILE);
      fail();
    } catch (NullPointerException ignore) {

    }
    try {
      StateUtils.writeStateToFile(
          new PersistentGlobalState(), Paths.get(folder.getRoot().getAbsolutePath()), null);
      fail();
    } catch (NullPointerException ignore) {

    }
  }

  @Test(expected = FileNotFoundException.class)
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

  @Test(expected = FileNotFoundException.class)
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
}
