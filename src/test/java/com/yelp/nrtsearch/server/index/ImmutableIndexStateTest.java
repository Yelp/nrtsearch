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
package com.yelp.nrtsearch.server.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt64Value;
import com.yelp.nrtsearch.server.config.IndexPreloadConfig;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.field.AtomFieldDef;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.FieldDefCreator;
import com.yelp.nrtsearch.server.field.IdFieldDef;
import com.yelp.nrtsearch.server.field.IntFieldDef;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.index.FieldUpdateUtils.UpdatedFieldInfo;
import com.yelp.nrtsearch.server.nrt.NrtDataManager;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.similarity.SimilarityCreator;
import com.yelp.nrtsearch.server.state.BackendGlobalState;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ImmutableIndexStateTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void setup() {
    String configFile = "nodeName: \"lucene_server_foo\"";
    NrtsearchConfig dummyConfig =
        new NrtsearchConfig(new ByteArrayInputStream(configFile.getBytes()));
    List<Plugin> dummyPlugins = Collections.emptyList();
    // these must be initialized to create an IndexState
    FieldDefCreator.initialize(dummyConfig, dummyPlugins);
    SimilarityCreator.initialize(dummyConfig, dummyPlugins);
  }

  private IndexStateInfo getEmptyState() {
    return IndexStateInfo.newBuilder()
        .setIndexName("test_index")
        .setCommitted(true)
        .setGen(1)
        .setLiveSettings(IndexLiveSettings.newBuilder().build())
        .setSettings(IndexSettings.newBuilder().build())
        .build();
  }

  private IndexStateInfo getStateWithSettings(IndexSettings indexSettings) {
    return getEmptyState().toBuilder().setSettings(indexSettings).build();
  }

  private IndexStateInfo getStateWithLiveSettings(IndexLiveSettings indexLiveSettings) {
    return getEmptyState().toBuilder().setLiveSettings(indexLiveSettings).build();
  }

  private ImmutableIndexState getIndexState(IndexStateInfo stateInfo) throws IOException {
    return getIndexState(stateInfo, new FieldAndFacetState());
  }

  private ImmutableIndexState getIndexState(IndexStateInfo stateInfo, FieldAndFacetState fieldState)
      throws IOException {
    return getIndexState(stateInfo, fieldState, new HashMap<>());
  }

  private ImmutableIndexState getIndexState(
      IndexStateInfo stateInfo, FieldAndFacetState fieldState, Map<Integer, ShardState> shards)
      throws IOException {
    IndexStateManager mockManager = mock(IndexStateManager.class);
    GlobalState mockGlobalState = mock(GlobalState.class);

    String configFile = "nodeName: \"lucene_server_foo\"";
    NrtsearchConfig dummyConfig =
        new NrtsearchConfig(new ByteArrayInputStream(configFile.getBytes()));

    when(mockGlobalState.getIndexDirBase()).thenReturn(folder.getRoot().toPath());
    when(mockGlobalState.getConfiguration()).thenReturn(dummyConfig);
    when(mockGlobalState.getThreadPoolConfiguration())
        .thenReturn(dummyConfig.getThreadPoolConfiguration());
    when(mockGlobalState.getFetchExecutor()).thenReturn(mock(ExecutorService.class));
    return new ImmutableIndexState(
        mockManager,
        mockGlobalState,
        "test_index",
        BackendGlobalState.getUniqueIndexName("test_index", "test_id"),
        stateInfo,
        fieldState,
        IndexLiveSettings.newBuilder().build(),
        shards);
  }

  private IndexWriterConfig getWriterConfigForSettings(IndexSettings settings) throws IOException {
    ImmutableIndexState indexState = getIndexState(getStateWithSettings(settings));
    Directory mockDirectory = mock(Directory.class);
    when(mockDirectory.listAll()).thenReturn(new String[0]);
    return indexState.getIndexWriterConfig(OpenMode.CREATE_OR_APPEND, mockDirectory, 0);
  }

  private ConcurrentMergeScheduler getMergeSchedulerForSettings(IndexSettings settings)
      throws IOException {
    IndexWriterConfig writerConfig = getWriterConfigForSettings(settings);
    return (ConcurrentMergeScheduler) writerConfig.getMergeScheduler();
  }

  private void verifyDoubleSetting(
      double expected,
      Function<ImmutableIndexState, Double> getFunc,
      Consumer<IndexSettings.Builder> setMessage)
      throws IOException {
    IndexSettings.Builder builder = IndexSettings.newBuilder();
    setMessage.accept(builder);
    assertEquals(
        expected, getFunc.apply(getIndexState(getStateWithSettings(builder.build()))), 0.0);
  }

  private void verifyDoubleLiveSetting(
      double expected,
      Function<ImmutableIndexState, Double> getFunc,
      Consumer<IndexLiveSettings.Builder> setMessage)
      throws IOException {
    IndexLiveSettings.Builder builder = IndexLiveSettings.newBuilder();
    setMessage.accept(builder);
    assertEquals(
        expected, getFunc.apply(getIndexState(getStateWithLiveSettings(builder.build()))), 0.0);
  }

  private void verifyBoolLiveSetting(
      boolean expected,
      Function<ImmutableIndexState, Boolean> getFunc,
      Consumer<IndexLiveSettings.Builder> setMessage)
      throws IOException {
    IndexLiveSettings.Builder builder = IndexLiveSettings.newBuilder();
    setMessage.accept(builder);
    assertEquals(expected, getFunc.apply(getIndexState(getStateWithLiveSettings(builder.build()))));
  }

  private void verifyIntLiveSetting(
      int expected,
      Function<ImmutableIndexState, Integer> getFunc,
      Consumer<IndexLiveSettings.Builder> setMessage)
      throws IOException {
    IndexLiveSettings.Builder builder = IndexLiveSettings.newBuilder();
    setMessage.accept(builder);
    assertEquals(
        Integer.valueOf(expected),
        getFunc.apply(getIndexState(getStateWithLiveSettings(builder.build()))));
  }

  private void verifyLongLiveSetting(
      long expected,
      Function<ImmutableIndexState, Long> getFunc,
      Consumer<IndexLiveSettings.Builder> setMessage)
      throws IOException {
    IndexLiveSettings.Builder builder = IndexLiveSettings.newBuilder();
    setMessage.accept(builder);
    assertEquals(
        Long.valueOf(expected),
        getFunc.apply(getIndexState(getStateWithLiveSettings(builder.build()))));
  }

  private void assertSettingException(
      String expectedMsg, Consumer<IndexSettings.Builder> setMessage) throws IOException {
    IndexSettings.Builder builder = IndexSettings.newBuilder();
    setMessage.accept(builder);
    try {
      getIndexState(getStateWithSettings(builder.build()));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(expectedMsg, e.getMessage());
    }
  }

  private void assertLiveSettingException(
      String expectedMsg, Consumer<IndexLiveSettings.Builder> setMessage) throws IOException {
    IndexLiveSettings.Builder builder = IndexLiveSettings.newBuilder();
    setMessage.accept(builder);
    try {
      getIndexState(getStateWithLiveSettings(builder.build()));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(expectedMsg, e.getMessage());
    }
  }

  private DoubleValue wrap(double value) {
    return DoubleValue.newBuilder().setValue(value).build();
  }

  private BoolValue wrap(boolean value) {
    return BoolValue.newBuilder().setValue(value).build();
  }

  private Int32Value wrap(int value) {
    return Int32Value.newBuilder().setValue(value).build();
  }

  private UInt64Value wrap(long value) {
    return UInt64Value.newBuilder().setValue(value).build();
  }

  private StringValue wrap(String value) {
    return StringValue.newBuilder().setValue(value).build();
  }

  @Test
  public void testNrtCachingDirectoryMaxMergeSizeMB_default() throws IOException {
    assertEquals(
        ImmutableIndexState.DEFAULT_NRT_CACHING_MAX_MERGE_SIZE_MB,
        getIndexState(getEmptyState()).getNrtCachingDirectoryMaxMergeSizeMB(),
        0.0);
  }

  @Test
  public void testNrtCachingDirectoryMaxMergeSizeMB_set() throws IOException {
    verifyDoubleSetting(
        0.0,
        ImmutableIndexState::getNrtCachingDirectoryMaxMergeSizeMB,
        b -> b.setNrtCachingDirectoryMaxMergeSizeMB(wrap(0.0)));
    verifyDoubleSetting(
        10.0,
        ImmutableIndexState::getNrtCachingDirectoryMaxMergeSizeMB,
        b -> b.setNrtCachingDirectoryMaxMergeSizeMB(wrap(10.0)));
  }

  @Test
  public void testNrtCachingDirectoryMaxMergeSizeMB_invalid() throws IOException {
    String expectedMsg = "nrtCachingDirectoryMaxMergeSizeMB must be >= 0";
    assertSettingException(expectedMsg, b -> b.setNrtCachingDirectoryMaxMergeSizeMB(wrap(-1.0)));
  }

  @Test
  public void testNrtCachingDirectoryMaxSizeMB_default() throws IOException {
    assertEquals(
        ImmutableIndexState.DEFAULT_NRT_CACHING_MAX_SIZE_MB,
        getIndexState(getEmptyState()).getNrtCachingDirectoryMaxSizeMB(),
        0.0);
  }

  @Test
  public void testNrtCachingDirectoryMaxSizeMB_set() throws IOException {
    verifyDoubleSetting(
        0.0,
        ImmutableIndexState::getNrtCachingDirectoryMaxSizeMB,
        b -> b.setNrtCachingDirectoryMaxSizeMB(wrap(0.0)));
    verifyDoubleSetting(
        100.0,
        ImmutableIndexState::getNrtCachingDirectoryMaxSizeMB,
        b -> b.setNrtCachingDirectoryMaxSizeMB(wrap(100.0)));
  }

  @Test
  public void testNrtCachingDirectoryMaxSizeMB_invalid() throws IOException {
    String expectedMsg = "nrtCachingDirectoryMaxSizeMB must be >= 0";
    assertSettingException(expectedMsg, b -> b.setNrtCachingDirectoryMaxSizeMB(wrap(-1.0)));
  }

  @Test
  public void testMergeSchedulerCounts_default() throws IOException {
    ConcurrentMergeScheduler mergeScheduler =
        getMergeSchedulerForSettings(IndexSettings.newBuilder().build());
    assertEquals(
        ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, mergeScheduler.getMaxMergeCount());
    assertEquals(
        ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS,
        mergeScheduler.getMaxThreadCount());
  }

  @Test
  public void testMergeSchedulerCounts_set() throws IOException {
    ConcurrentMergeScheduler mergeScheduler =
        getMergeSchedulerForSettings(
            IndexSettings.newBuilder()
                .setConcurrentMergeSchedulerMaxThreadCount(
                    Int32Value.newBuilder().setValue(1).build())
                .setConcurrentMergeSchedulerMaxMergeCount(
                    Int32Value.newBuilder().setValue(1).build())
                .build());
    assertEquals(1, mergeScheduler.getMaxMergeCount());
    assertEquals(1, mergeScheduler.getMaxThreadCount());
    mergeScheduler =
        getMergeSchedulerForSettings(
            IndexSettings.newBuilder()
                .setConcurrentMergeSchedulerMaxThreadCount(
                    Int32Value.newBuilder().setValue(10).build())
                .setConcurrentMergeSchedulerMaxMergeCount(
                    Int32Value.newBuilder().setValue(15).build())
                .build());
    assertEquals(15, mergeScheduler.getMaxMergeCount());
    assertEquals(10, mergeScheduler.getMaxThreadCount());
  }

  @Test
  public void testMergeSchedulerCounts_invalid() throws IOException {
    String expectedMsg =
        "both concurrentMergeSchedulerMaxMergeCount and concurrentMergeSchedulerMaxThreadCount must be AUTO_DETECT_MERGES_AND_THREADS (-1)";
    assertSettingException(expectedMsg, b -> b.setConcurrentMergeSchedulerMaxThreadCount(wrap(10)));
    assertSettingException(expectedMsg, b -> b.setConcurrentMergeSchedulerMaxMergeCount(wrap(10)));
    expectedMsg =
        "concurrentMergeSchedulerMaxThreadCount should be <= concurrentMergeSchedulerMaxMergeCount (= 5)";
    assertSettingException(
        expectedMsg,
        b -> {
          b.setConcurrentMergeSchedulerMaxThreadCount(wrap(10));
          b.setConcurrentMergeSchedulerMaxMergeCount(wrap(5));
        });
  }

  @Test
  public void testIndexSort_default() throws IOException {
    IndexWriterConfig writerConfig = getWriterConfigForSettings(IndexSettings.newBuilder().build());
    assertNull(writerConfig.getIndexSort());
  }

  @Test
  public void testIndexSort_set() throws IOException {
    Field sortField =
        Field.newBuilder()
            .setName("field1")
            .setType(FieldType.INT)
            .setStoreDocValues(true)
            .setSearch(true)
            .build();
    UpdatedFieldInfo fieldInfo =
        FieldUpdateUtils.updateFields(
            new FieldAndFacetState(), Collections.emptyMap(), Collections.singleton(sortField));
    ImmutableIndexState indexState =
        getIndexState(
            getStateWithSettings(
                IndexSettings.newBuilder()
                    .setIndexSort(
                        SortFields.newBuilder()
                            .addSortedFields(SortType.newBuilder().setFieldName("field1").build())
                            .build())
                    .build()),
            fieldInfo.fieldAndFacetState);

    Directory mockDirectory = mock(Directory.class);
    when(mockDirectory.listAll()).thenReturn(new String[0]);
    IndexWriterConfig writerConfig =
        indexState.getIndexWriterConfig(OpenMode.CREATE_OR_APPEND, mockDirectory, 0);
    assertNotNull(writerConfig.getIndexSort());
  }

  @Test
  public void testIndexSort_invalid() throws IOException {
    try {
      getIndexState(
          getStateWithSettings(
              IndexSettings.newBuilder()
                  .setIndexSort(
                      SortFields.newBuilder()
                          .addSortedFields(SortType.newBuilder().setFieldName("docid").build())
                          .build())
                  .build()));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Sort field: null, type: DOC is not in allowed types: [STRING, INT, FLOAT, LONG, DOUBLE]",
          e.getMessage());
    }
  }

  @Test
  public void testIndexMergeSchedulerAutoThrottle_default() throws IOException {
    ConcurrentMergeScheduler mergeScheduler =
        getMergeSchedulerForSettings(IndexSettings.newBuilder().build());
    assertEquals(Boolean.FALSE, mergeScheduler.getAutoIOThrottle());
  }

  @Test
  public void testIndexMergeSchedulerAutoThrottle_set() throws IOException {
    ConcurrentMergeScheduler mergeScheduler =
        getMergeSchedulerForSettings(
            IndexSettings.newBuilder()
                .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
                .build());
    assertEquals(Boolean.TRUE, mergeScheduler.getAutoIOThrottle());
  }

  @Test
  public void testDirectory_default() throws IOException {
    DirectoryFactory factory = getIndexState(getEmptyState()).getDirectoryFactory();
    Directory directory =
        factory.open(
            folder.getRoot().toPath(), new IndexPreloadConfig(false, Collections.emptySet()));
    assertTrue(directory instanceof FSDirectory);
  }

  @Test
  public void testDirectory_set() throws IOException {
    DirectoryFactory factory =
        getIndexState(
                getStateWithSettings(
                    IndexSettings.newBuilder().setDirectory(wrap("NIOFSDirectory")).build()))
            .getDirectoryFactory();
    Directory directory =
        factory.open(
            folder.getRoot().toPath(), new IndexPreloadConfig(false, Collections.emptySet()));
    assertTrue(directory instanceof NIOFSDirectory);
  }

  @Test
  public void testDirectory_invalid() throws IOException {
    String expectedMsg = "could not locate Directory sub-class \"Invalid\"; verify CLASSPATH";
    assertSettingException(expectedMsg, b -> b.setDirectory(wrap("Invalid")));
  }

  @Test
  public void testRefreshSec_default() throws IOException {
    assertEquals(
        ImmutableIndexState.DEFAULT_MAX_REFRESH_SEC,
        getIndexState(getEmptyState()).getMaxRefreshSec(),
        0.0);
    assertEquals(
        ImmutableIndexState.DEFAULT_MIN_REFRESH_SEC,
        getIndexState(getEmptyState()).getMinRefreshSec(),
        0.0);
  }

  @Test
  public void testRefreshSec_set() throws IOException {
    verifyDoubleLiveSetting(
        10.0, ImmutableIndexState::getMaxRefreshSec, b -> b.setMaxRefreshSec(wrap(10.0)));
    verifyDoubleLiveSetting(
        0.5, ImmutableIndexState::getMinRefreshSec, b -> b.setMinRefreshSec(wrap(0.5)));
    verifyDoubleLiveSetting(
        5.0,
        ImmutableIndexState::getMaxRefreshSec,
        b -> {
          b.setMinRefreshSec(wrap(5.0));
          b.setMaxRefreshSec(wrap(5.0));
        });
  }

  @Test
  public void testRefreshSec_invalid() throws IOException {
    String expectedMsg = "maxRefreshSec must be >= minRefreshSec";
    assertLiveSettingException(
        expectedMsg,
        b -> {
          b.setMaxRefreshSec(wrap(5.0));
          b.setMinRefreshSec(wrap(10.0));
        });
  }

  @Test
  public void testMaxSearcherAgeSec_default() throws IOException {
    assertEquals(
        ImmutableIndexState.DEFAULT_MAX_SEARCHER_AGE,
        getIndexState(getEmptyState()).getMaxSearcherAgeSec(),
        0.0);
  }

  @Test
  public void testMaxSearcherAgeSec_set() throws IOException {
    verifyDoubleLiveSetting(
        100.0, ImmutableIndexState::getMaxSearcherAgeSec, b -> b.setMaxSearcherAgeSec(wrap(100.0)));
    verifyDoubleLiveSetting(
        0.0, ImmutableIndexState::getMaxSearcherAgeSec, b -> b.setMaxSearcherAgeSec(wrap(0.0)));
  }

  @Test
  public void testMaxSearcherAgeSec_invalid() throws IOException {
    String expectedMsg = "maxSearcherAgeSec must be >= 0.0";
    assertLiveSettingException(expectedMsg, b -> b.setMaxSearcherAgeSec(wrap(-1.0)));
  }

  @Test
  public void testIndexRamBufferSizeMB_default() throws IOException {
    assertEquals(
        ImmutableIndexState.DEFAULT_INDEX_RAM_BUFFER_SIZE_MB,
        getIndexState(getEmptyState()).getIndexRamBufferSizeMB(),
        0.0);
  }

  @Test
  public void testIndexRamBufferSizeMB_set() throws IOException {
    verifyDoubleLiveSetting(
        128.0,
        ImmutableIndexState::getIndexRamBufferSizeMB,
        b -> b.setIndexRamBufferSizeMB(wrap(128.0)));
    verifyDoubleLiveSetting(
        0.01,
        ImmutableIndexState::getIndexRamBufferSizeMB,
        b -> b.setIndexRamBufferSizeMB(wrap(0.01)));
  }

  @Test
  public void testIndexRamBufferSizeMB_invalid() throws IOException {
    String expectedMsg = "indexRamBufferSizeMB must be > 0.0";
    assertLiveSettingException(expectedMsg, b -> b.setIndexRamBufferSizeMB(wrap(0.0)));
  }

  @Test
  public void testAddDocumentsMaxBufferLen_default() throws IOException {
    assertEquals(
        ImmutableIndexState.DEFAULT_ADD_DOCS_MAX_BUFFER_LEN,
        getIndexState(getEmptyState()).getAddDocumentsMaxBufferLen());
  }

  @Test
  public void testAddDocumentsMaxBufferLen_set() throws IOException {
    verifyIntLiveSetting(
        500,
        ImmutableIndexState::getAddDocumentsMaxBufferLen,
        b -> b.setAddDocumentsMaxBufferLen(wrap(500)));
    verifyIntLiveSetting(
        1,
        ImmutableIndexState::getAddDocumentsMaxBufferLen,
        b -> b.setAddDocumentsMaxBufferLen(wrap(1)));
  }

  @Test
  public void testAddDocumentsMaxBufferLen_invalid() throws IOException {
    String expectedMsg = "addDocumentsMaxBufferLen must be > 0";
    assertLiveSettingException(expectedMsg, b -> b.setAddDocumentsMaxBufferLen(wrap(0)));
  }

  @Test
  public void testSliceMaxDocs_default() throws IOException {
    assertEquals(
        ImmutableIndexState.DEFAULT_SLICE_MAX_DOCS,
        getIndexState(getEmptyState()).getSliceMaxDocs());
  }

  @Test
  public void testSliceMaxDocs_set() throws IOException {
    verifyIntLiveSetting(
        1000, ImmutableIndexState::getSliceMaxDocs, b -> b.setSliceMaxDocs(wrap(1000)));
    verifyIntLiveSetting(1, ImmutableIndexState::getSliceMaxDocs, b -> b.setSliceMaxDocs(wrap(1)));
  }

  @Test
  public void testSliceMaxDocs_invalid() throws IOException {
    String expectedMsg = "sliceMaxDocs must be > 0";
    assertLiveSettingException(expectedMsg, b -> b.setSliceMaxDocs(wrap(0)));
  }

  @Test
  public void testSliceMaxSegments_default() throws IOException {
    assertEquals(
        ImmutableIndexState.DEFAULT_SLICE_MAX_SEGMENTS,
        getIndexState(getEmptyState()).getSliceMaxSegments());
  }

  @Test
  public void testSliceMaxSegments_set() throws IOException {
    verifyIntLiveSetting(
        100, ImmutableIndexState::getSliceMaxSegments, b -> b.setSliceMaxSegments(wrap(100)));
    verifyIntLiveSetting(
        1, ImmutableIndexState::getSliceMaxSegments, b -> b.setSliceMaxSegments(wrap(1)));
  }

  @Test
  public void testSliceMaxSegments_invalid() throws IOException {
    String expectedMsg = "sliceMaxSegments must be > 0";
    assertLiveSettingException(expectedMsg, b -> b.setSliceMaxSegments(wrap(0)));
  }

  private ImmutableIndexState getIndexStateForVirtualShading(
      boolean enabled, IndexLiveSettings settings) throws IOException {
    IndexStateManager mockManager = mock(IndexStateManager.class);
    GlobalState mockGlobalState = mock(GlobalState.class);

    String configFile = "virtualSharding: " + (enabled ? "true" : "false");
    NrtsearchConfig dummyConfig =
        new NrtsearchConfig(new ByteArrayInputStream(configFile.getBytes()));

    when(mockGlobalState.getIndexDirBase()).thenReturn(folder.getRoot().toPath());
    when(mockGlobalState.getConfiguration()).thenReturn(dummyConfig);
    when(mockGlobalState.getThreadPoolConfiguration())
        .thenReturn(dummyConfig.getThreadPoolConfiguration());
    when(mockGlobalState.getFetchExecutor()).thenReturn(mock(ExecutorService.class));
    return new ImmutableIndexState(
        mockManager,
        mockGlobalState,
        "test_index",
        BackendGlobalState.getUniqueIndexName("test_index", "test_id"),
        getStateWithLiveSettings(settings),
        new FieldAndFacetState(),
        IndexLiveSettings.newBuilder().build(),
        new HashMap<>());
  }

  @Test
  public void testVirtualShards_default() throws IOException {
    ImmutableIndexState indexState =
        getIndexStateForVirtualShading(true, IndexLiveSettings.newBuilder().build());
    assertEquals(ImmutableIndexState.DEFAULT_VIRTUAL_SHARDS, indexState.getVirtualShards());
  }

  @Test
  public void testVirtualShardsEnabled_set() throws IOException {
    ImmutableIndexState indexState =
        getIndexStateForVirtualShading(
            true, IndexLiveSettings.newBuilder().setVirtualShards(wrap(100)).build());
    assertEquals(100, indexState.getVirtualShards());
    indexState =
        getIndexStateForVirtualShading(
            true, IndexLiveSettings.newBuilder().setVirtualShards(wrap(1)).build());
    assertEquals(1, indexState.getVirtualShards());
  }

  @Test
  public void testVirtualShardsDisabled_set() throws IOException {
    ImmutableIndexState indexState =
        getIndexStateForVirtualShading(
            false, IndexLiveSettings.newBuilder().setVirtualShards(wrap(100)).build());
    assertEquals(1, indexState.getVirtualShards());
    indexState =
        getIndexStateForVirtualShading(
            false, IndexLiveSettings.newBuilder().setVirtualShards(wrap(1)).build());
    assertEquals(1, indexState.getVirtualShards());
  }

  @Test
  public void testVirtualShards_invalid() throws IOException {
    String expectedMsg = "virtualShards must be > 0";
    assertLiveSettingException(expectedMsg, b -> b.setVirtualShards(wrap(0)));
  }

  @Test
  public void testMaxMergedSegmentMB_default() throws IOException {
    assertEquals(
        ImmutableIndexState.DEFAULT_MAX_MERGED_SEGMENT_MB,
        getIndexState(getEmptyState()).getMaxMergedSegmentMB());
  }

  @Test
  public void testMaxMergedSegmentMB_set() throws IOException {
    verifyIntLiveSetting(
        1000, ImmutableIndexState::getMaxMergedSegmentMB, b -> b.setMaxMergedSegmentMB(wrap(1000)));
    verifyIntLiveSetting(
        0, ImmutableIndexState::getMaxMergedSegmentMB, b -> b.setMaxMergedSegmentMB(wrap(0)));
  }

  @Test
  public void testMaxMergedSegmentMB_invalid() throws IOException {
    String expectedMsg = "maxMergedSegmentMB must be >= 0";
    assertLiveSettingException(expectedMsg, b -> b.setMaxMergedSegmentMB(wrap(-1)));
  }

  @Test
  public void testSegmentsPerTier_default() throws IOException {
    assertEquals(
        ImmutableIndexState.DEFAULT_SEGMENTS_PER_TIER,
        getIndexState(getEmptyState()).getSegmentsPerTier());
  }

  @Test
  public void testSegmentsPerTier_set() throws IOException {
    verifyIntLiveSetting(
        50, ImmutableIndexState::getSegmentsPerTier, b -> b.setSegmentsPerTier(wrap(50)));
    verifyIntLiveSetting(
        2, ImmutableIndexState::getSegmentsPerTier, b -> b.setSegmentsPerTier(wrap(2)));
  }

  @Test
  public void testSegmentsPerTier_invalid() throws IOException {
    String expectedMsg = "segmentsPerTier must be >= 2";
    assertLiveSettingException(expectedMsg, b -> b.setSegmentsPerTier(wrap(1)));
  }

  @Test
  public void testDefaultSearchTimeoutSec_default() throws IOException {
    assertEquals(0.0, getIndexState(getEmptyState()).getDefaultSearchTimeoutSec(), 0.0);
  }

  @Test
  public void testDefaultSearchTimeoutSec_set() throws IOException {
    verifyDoubleLiveSetting(
        5.0,
        ImmutableIndexState::getDefaultSearchTimeoutSec,
        b -> b.setDefaultSearchTimeoutSec(wrap(5.0)));
  }

  @Test
  public void testDefaultSearchTimeoutSec_invalid() throws IOException {
    String expectedMsg = "defaultSearchTimeoutSec must be >= 0.0";
    assertLiveSettingException(expectedMsg, b -> b.setDefaultSearchTimeoutSec(wrap(-1.0)));
  }

  @Test
  public void testDefaultSearchTimeoutCheckEvery_default() throws IOException {
    assertEquals(0, getIndexState(getEmptyState()).getDefaultSearchTimeoutCheckEvery());
  }

  @Test
  public void testDefaultSearchTimeoutCheckEvery_set() throws IOException {
    verifyIntLiveSetting(
        100,
        ImmutableIndexState::getDefaultSearchTimeoutCheckEvery,
        b -> b.setDefaultSearchTimeoutCheckEvery(wrap(100)));
  }

  @Test
  public void testDefaultSearchTimeoutCheckEvery_invalid() throws IOException {
    String expectedMsg = "defaultSearchTimeoutCheckEvery must be >= 0";
    assertLiveSettingException(expectedMsg, b -> b.setDefaultSearchTimeoutCheckEvery(wrap(-1)));
  }

  @Test
  public void testDefaultTerminateAfter_default() throws IOException {
    assertEquals(0, getIndexState(getEmptyState()).getDefaultTerminateAfter());
  }

  @Test
  public void testDefaultTerminateAfter_set() throws IOException {
    verifyIntLiveSetting(
        100,
        ImmutableIndexState::getDefaultTerminateAfter,
        b -> b.setDefaultTerminateAfter(wrap(100)));
  }

  @Test
  public void testDefaultTerminateAfter_invalid() throws IOException {
    String expectedMsg = "defaultTerminateAfter must be >= 0";
    assertLiveSettingException(expectedMsg, b -> b.setDefaultTerminateAfter(wrap(-1)));
  }

  @Test
  public void testMaxMergePreCopyDurationSec_default() throws IOException {
    assertEquals(0, getIndexState(getEmptyState()).getMaxMergePreCopyDurationSec());
  }

  @Test
  public void testMaxMergePreCopyDurationSec_set() throws IOException {
    verifyLongLiveSetting(
        0,
        ImmutableIndexState::getMaxMergePreCopyDurationSec,
        b -> b.setMaxMergePreCopyDurationSec(wrap(0L)));
    verifyLongLiveSetting(
        100,
        ImmutableIndexState::getMaxMergePreCopyDurationSec,
        b -> b.setMaxMergePreCopyDurationSec(wrap(100L)));
  }

  @Test
  public void testMaxMergePreCopyDurationSec_invalid() throws IOException {
    String expectedMsg = "maxMergePreCopyDurationSec must be >= 0";
    assertLiveSettingException(expectedMsg, b -> b.setMaxMergePreCopyDurationSec(wrap(-1L)));
  }

  @Test
  public void testVerboseMetrics_default() throws IOException {
    assertFalse(getIndexState(getEmptyState()).getVerboseMetrics());
  }

  @Test
  public void testVerboseMetrics_set() throws IOException {
    verifyBoolLiveSetting(
        true, ImmutableIndexState::getVerboseMetrics, b -> b.setVerboseMetrics(wrap(true)));
  }

  @Test
  public void testParallelFetchConfig_default() throws IOException {
    IndexState.ParallelFetchConfig parallelFetchConfig =
        getIndexState(getEmptyState()).getParallelFetchConfig();
    assertEquals(1, parallelFetchConfig.maxParallelism());
    assertFalse(parallelFetchConfig.parallelFetchByField());
    assertEquals(50, parallelFetchConfig.parallelFetchChunkSize());
    assertNotNull(parallelFetchConfig.fetchExecutor());
  }

  @Test
  public void testParallelFetchConfig_setParallelFetchByField() throws IOException {
    IndexState.ParallelFetchConfig parallelFetchConfig =
        getIndexState(
                getStateWithLiveSettings(
                    IndexLiveSettings.newBuilder()
                        .setParallelFetchByField(BoolValue.newBuilder().setValue(true).build())
                        .build()))
            .getParallelFetchConfig();
    assertEquals(1, parallelFetchConfig.maxParallelism());
    assertTrue(parallelFetchConfig.parallelFetchByField());
    assertEquals(50, parallelFetchConfig.parallelFetchChunkSize());
    assertNotNull(parallelFetchConfig.fetchExecutor());
  }

  @Test
  public void testParallelFetchConfig_setParallelFetchChunkSize() throws IOException {
    IndexState.ParallelFetchConfig parallelFetchConfig =
        getIndexState(
                getStateWithLiveSettings(
                    IndexLiveSettings.newBuilder()
                        .setParallelFetchChunkSize(Int32Value.newBuilder().setValue(100).build())
                        .build()))
            .getParallelFetchConfig();
    assertEquals(1, parallelFetchConfig.maxParallelism());
    assertFalse(parallelFetchConfig.parallelFetchByField());
    assertEquals(100, parallelFetchConfig.parallelFetchChunkSize());
    assertNotNull(parallelFetchConfig.fetchExecutor());
  }

  @Test
  public void testParallelFetchConfig_invalidParallelFetchChunkSize() throws IOException {
    try {
      getIndexState(
              getStateWithLiveSettings(
                  IndexLiveSettings.newBuilder()
                      .setParallelFetchChunkSize(Int32Value.newBuilder().setValue(0).build())
                      .build()))
          .getParallelFetchConfig();
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("parallelFetchChunkSize must be > 0", e.getMessage());
    }
  }

  @Test
  public void testParallelFetchConfig_maxParallelism() throws IOException {
    IndexStateManager mockManager = mock(IndexStateManager.class);
    GlobalState mockGlobalState = mock(GlobalState.class);

    String configFile = "threadPoolConfiguration:\n  fetch:\n    maxThreads: 10";
    NrtsearchConfig config = new NrtsearchConfig(new ByteArrayInputStream(configFile.getBytes()));

    when(mockGlobalState.getIndexDirBase()).thenReturn(folder.getRoot().toPath());
    when(mockGlobalState.getConfiguration()).thenReturn(config);
    when(mockGlobalState.getThreadPoolConfiguration())
        .thenReturn(config.getThreadPoolConfiguration());
    when(mockGlobalState.getFetchExecutor()).thenReturn(mock(ExecutorService.class));
    IndexState indexState =
        new ImmutableIndexState(
            mockManager,
            mockGlobalState,
            "test_index",
            BackendGlobalState.getUniqueIndexName("test_index", "test_id"),
            IndexStateInfo.newBuilder().build(),
            new FieldAndFacetState(),
            IndexLiveSettings.newBuilder().build(),
            Map.of());

    IndexState.ParallelFetchConfig parallelFetchConfig = indexState.getParallelFetchConfig();
    assertEquals(10, parallelFetchConfig.maxParallelism());
    assertFalse(parallelFetchConfig.parallelFetchByField());
    assertEquals(50, parallelFetchConfig.parallelFetchChunkSize());
    assertNotNull(parallelFetchConfig.fetchExecutor());
  }

  @Test
  public void testGetCurrentStateInfo() throws IOException {
    IndexStateInfo indexStateInfo = getEmptyState();
    ImmutableIndexState indexState = getIndexState(indexStateInfo);
    assertSame(indexStateInfo, indexState.getCurrentStateInfo());
  }

  @Test
  public void testGetMergedSettings() throws IOException {
    IndexSettings settings =
        IndexSettings.newBuilder()
            .setConcurrentMergeSchedulerMaxMergeCount(wrap(5))
            .setConcurrentMergeSchedulerMaxThreadCount(wrap(2))
            .setNrtCachingDirectoryMaxSizeMB(wrap(100.0))
            .build();
    ImmutableIndexState indexState = getIndexState(getStateWithSettings(settings));
    IndexSettings expectedMergedSettings =
        ImmutableIndexState.DEFAULT_INDEX_SETTINGS.toBuilder()
            .setConcurrentMergeSchedulerMaxMergeCount(wrap(5))
            .setConcurrentMergeSchedulerMaxThreadCount(wrap(2))
            .setNrtCachingDirectoryMaxSizeMB(wrap(100.0))
            .build();
    assertEquals(expectedMergedSettings, indexState.getMergedSettings());
  }

  @Test
  public void testGetMergedLiveSettings() throws IOException {
    IndexLiveSettings liveSettings =
        IndexLiveSettings.newBuilder()
            .setDefaultTerminateAfter(wrap(100))
            .setMaxRefreshSec(wrap(10.0))
            .setSegmentsPerTier(wrap(5))
            .build();
    ImmutableIndexState indexState = getIndexState(getStateWithLiveSettings(liveSettings));
    IndexLiveSettings expectedMergedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS.toBuilder()
            .setDefaultTerminateAfter(wrap(100))
            .setMaxRefreshSec(wrap(10.0))
            .setSegmentsPerTier(wrap(5))
            .build();
    assertEquals(expectedMergedSettings, indexState.getMergedLiveSettings(false));
    assertEquals(expectedMergedSettings, indexState.getMergedLiveSettings(true));
  }

  private ImmutableIndexState getIndexStateForLiveSettingsOverrides(
      IndexLiveSettings settings, IndexLiveSettings overrides) throws IOException {
    IndexStateManager mockManager = mock(IndexStateManager.class);
    GlobalState mockGlobalState = mock(GlobalState.class);

    String configFile = "nodeName: \"lucene_server_foo\"";
    NrtsearchConfig dummyConfig =
        new NrtsearchConfig(new ByteArrayInputStream(configFile.getBytes()));

    when(mockGlobalState.getIndexDirBase()).thenReturn(folder.getRoot().toPath());
    when(mockGlobalState.getConfiguration()).thenReturn(dummyConfig);
    when(mockGlobalState.getThreadPoolConfiguration())
        .thenReturn(dummyConfig.getThreadPoolConfiguration());
    when(mockGlobalState.getFetchExecutor()).thenReturn(mock(ExecutorService.class));
    return new ImmutableIndexState(
        mockManager,
        mockGlobalState,
        "test_index",
        BackendGlobalState.getUniqueIndexName("test_index", "test_id"),
        getStateWithLiveSettings(settings),
        new FieldAndFacetState(),
        overrides,
        new HashMap<>());
  }

  @Test
  public void testLiveSettingsOverrides() throws IOException {
    IndexLiveSettings liveSettings =
        IndexLiveSettings.newBuilder()
            .setDefaultTerminateAfter(wrap(100))
            .setMaxRefreshSec(wrap(10.0))
            .setSegmentsPerTier(wrap(5))
            .build();
    IndexLiveSettings liveSettingsOverrides =
        IndexLiveSettings.newBuilder()
            .setSliceMaxDocs(Int32Value.newBuilder().setValue(1).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(1).build())
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(10).build())
            .build();
    ImmutableIndexState indexState =
        getIndexStateForLiveSettingsOverrides(liveSettings, liveSettingsOverrides);
    IndexLiveSettings expectedMergedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS.toBuilder()
            .setDefaultTerminateAfter(wrap(100))
            .setMaxRefreshSec(wrap(10.0))
            .setSegmentsPerTier(wrap(5))
            .build();
    IndexLiveSettings expectedMergedSettingsWithLocal =
        expectedMergedSettings.toBuilder()
            .setSliceMaxDocs(Int32Value.newBuilder().setValue(1).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(1).build())
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(10).build())
            .build();
    assertEquals(expectedMergedSettings, indexState.getMergedLiveSettings(false));
    assertEquals(expectedMergedSettingsWithLocal, indexState.getMergedLiveSettings(true));

    assertEquals(1, indexState.getSliceMaxDocs());
    assertEquals(1, indexState.getSliceMaxSegments());
    assertEquals(10, indexState.getDefaultTerminateAfter());
  }

  @Test
  public void testGetFieldAndFacetState() throws IOException {
    FieldAndFacetState fieldAndFacetState = new FieldAndFacetState();
    ImmutableIndexState indexState = getIndexState(getEmptyState(), fieldAndFacetState);
    assertSame(fieldAndFacetState, indexState.getFieldAndFacetState());
  }

  @Test
  public void testGetField() throws IOException {
    Field field =
        Field.newBuilder()
            .setName("field1")
            .setType(FieldType.INT)
            .setStoreDocValues(true)
            .setSearch(true)
            .build();
    UpdatedFieldInfo fieldInfo =
        FieldUpdateUtils.updateFields(
            new FieldAndFacetState(), Collections.emptyMap(), Collections.singleton(field));
    IndexStateInfo indexStateInfo =
        getEmptyState().toBuilder().putAllFields(fieldInfo.fields).build();
    ImmutableIndexState indexState = getIndexState(indexStateInfo, fieldInfo.fieldAndFacetState);
    FieldDef fieldDef = indexState.getField("field1");
    assertTrue(fieldDef instanceof IntFieldDef);
  }

  @Test
  public void testGetMetaField() throws IOException {
    ImmutableIndexState indexState = getIndexState(getEmptyState());
    FieldDef fieldDef = indexState.getField(IndexState.NESTED_PATH);
    assertTrue(fieldDef instanceof AtomFieldDef);
  }

  @Test
  public void testGetFieldNotRegistered() throws IOException {
    ImmutableIndexState indexState = getIndexState(getEmptyState());
    assertNull(indexState.getField("invalid"));
  }

  @Test
  public void testGetFieldOrThrow() throws IOException {
    Field field =
        Field.newBuilder()
            .setName("field1")
            .setType(FieldType.INT)
            .setStoreDocValues(true)
            .setSearch(true)
            .build();
    UpdatedFieldInfo fieldInfo =
        FieldUpdateUtils.updateFields(
            new FieldAndFacetState(), Collections.emptyMap(), Collections.singleton(field));
    IndexStateInfo indexStateInfo =
        getEmptyState().toBuilder().putAllFields(fieldInfo.fields).build();
    ImmutableIndexState indexState = getIndexState(indexStateInfo, fieldInfo.fieldAndFacetState);
    FieldDef fieldDef = indexState.getFieldOrThrow("field1");
    assertTrue(fieldDef instanceof IntFieldDef);
  }

  @Test
  public void testGetMetaFieldOrThrow() throws IOException {
    ImmutableIndexState indexState = getIndexState(getEmptyState());
    FieldDef fieldDef = indexState.getFieldOrThrow(IndexState.NESTED_PATH);
    assertTrue(fieldDef instanceof AtomFieldDef);
  }

  @Test
  public void testGetFieldOrThrowNotRegistered() throws IOException {
    ImmutableIndexState indexState = getIndexState(getEmptyState());
    try {
      indexState.getFieldOrThrow("invalid");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("field \"invalid\" is unknown", e.getMessage());
    }
  }

  @Test
  public void testGetAllFields() throws IOException {
    Field field =
        Field.newBuilder()
            .setName("field1")
            .setType(FieldType.INT)
            .setStoreDocValues(true)
            .setSearch(true)
            .build();
    UpdatedFieldInfo fieldInfo =
        FieldUpdateUtils.updateFields(
            new FieldAndFacetState(), Collections.emptyMap(), Collections.singleton(field));
    IndexStateInfo indexStateInfo =
        getEmptyState().toBuilder().putAllFields(fieldInfo.fields).build();
    ImmutableIndexState indexState = getIndexState(indexStateInfo, fieldInfo.fieldAndFacetState);
    assertSame(fieldInfo.fieldAndFacetState.getFields(), indexState.getAllFields());
  }

  @Test
  public void testGetAllFieldsJSON() throws IOException {
    Field field =
        Field.newBuilder()
            .setName("field1")
            .setType(FieldType.INT)
            .setStoreDocValues(true)
            .setSearch(true)
            .build();
    UpdatedFieldInfo fieldInfo =
        FieldUpdateUtils.updateFields(
            new FieldAndFacetState(), Collections.emptyMap(), Collections.singleton(field));
    IndexStateInfo indexStateInfo =
        getEmptyState().toBuilder().putAllFields(fieldInfo.fields).build();
    ImmutableIndexState indexState = getIndexState(indexStateInfo, fieldInfo.fieldAndFacetState);
    String fieldJson = indexState.getAllFieldsJSON();
    @SuppressWarnings("unchecked")
    Map<String, Object> fieldsMap =
        (Map<String, Object>) new ObjectMapper().readValue(fieldJson, Map.class);
    assertEquals(Set.of("field1"), fieldsMap.keySet());
  }

  @Test
  public void testGetIdFieldDef() throws IOException {
    FieldAndFacetState mockFieldState = mock(FieldAndFacetState.class);
    Optional<IdFieldDef> idFieldOption = Optional.empty();
    when(mockFieldState.getIdFieldDef()).thenReturn(idFieldOption);
    ImmutableIndexState indexState = getIndexState(getEmptyState(), mockFieldState);
    assertSame(idFieldOption, indexState.getIdFieldDef());
    verify(mockFieldState, times(1)).getIdFieldDef();
    verifyNoMoreInteractions(mockFieldState);
  }

  @Test
  public void testGetIndexAnalyzedFields() throws IOException {
    FieldAndFacetState mockFieldState = mock(FieldAndFacetState.class);
    List<String> analyzedFields = List.of("field1");
    when(mockFieldState.getIndexedAnalyzedFields()).thenReturn(analyzedFields);
    ImmutableIndexState indexState = getIndexState(getEmptyState(), mockFieldState);
    assertSame(analyzedFields, indexState.getIndexedAnalyzedFields());
    verify(mockFieldState, times(1)).getIndexedAnalyzedFields();
    verifyNoMoreInteractions(mockFieldState);
  }

  @Test
  public void testGetEagerGlobalOrdinalFields() throws IOException {
    FieldAndFacetState mockFieldState = mock(FieldAndFacetState.class);
    Map<String, FieldDef> eagerOrdinalFields = new HashMap<>();
    when(mockFieldState.getEagerGlobalOrdinalFields()).thenReturn(eagerOrdinalFields);
    ImmutableIndexState indexState = getIndexState(getEmptyState(), mockFieldState);
    assertSame(eagerOrdinalFields, indexState.getEagerGlobalOrdinalFields());
    verify(mockFieldState, times(1)).getEagerGlobalOrdinalFields();
    verifyNoMoreInteractions(mockFieldState);
  }

  @Test
  public void testHasNestedChildFields() throws IOException {
    FieldAndFacetState mockFieldState = mock(FieldAndFacetState.class);
    when(mockFieldState.getHasNestedChildFields()).thenReturn(true);
    ImmutableIndexState indexState = getIndexState(getEmptyState(), mockFieldState);
    assertTrue(indexState.hasNestedChildFields());
    verify(mockFieldState, times(1)).getHasNestedChildFields();
    verifyNoMoreInteractions(mockFieldState);
  }

  @Test
  public void testHasFacets() throws IOException {
    FieldAndFacetState mockFieldState = mock(FieldAndFacetState.class);
    Set<String> internalFacetNames = Set.of("$field1");
    when(mockFieldState.getInternalFacetFieldNames()).thenReturn(internalFacetNames);
    ImmutableIndexState indexState = getIndexState(getEmptyState(), mockFieldState);
    assertTrue(indexState.hasFacets());
    verify(mockFieldState, times(1)).getInternalFacetFieldNames();
    verifyNoMoreInteractions(mockFieldState);
  }

  @Test
  public void testGetInternalFacetNames() throws IOException {
    FieldAndFacetState mockFieldState = mock(FieldAndFacetState.class);
    Set<String> internalFacetNames = Set.of("$field1");
    when(mockFieldState.getInternalFacetFieldNames()).thenReturn(internalFacetNames);
    ImmutableIndexState indexState = getIndexState(getEmptyState(), mockFieldState);
    assertSame(internalFacetNames, indexState.getInternalFacetFieldNames());
    verify(mockFieldState, times(1)).getInternalFacetFieldNames();
    verifyNoMoreInteractions(mockFieldState);
  }

  @Test
  public void testGetFacetsConfig() throws IOException {
    FieldAndFacetState mockFieldState = mock(FieldAndFacetState.class);
    FacetsConfig mockConfig = mock(FacetsConfig.class);
    when(mockFieldState.getFacetsConfig()).thenReturn(mockConfig);
    ImmutableIndexState indexState = getIndexState(getEmptyState(), mockFieldState);
    assertSame(mockConfig, indexState.getFacetsConfig());
    verify(mockFieldState, times(1)).getFacetsConfig();
    verifyNoMoreInteractions(mockFieldState);
  }

  @Test
  public void testGetShards() throws IOException {
    Map<Integer, ShardState> shardStateMap = new HashMap<>();
    ImmutableIndexState indexState =
        getIndexState(getEmptyState(), new FieldAndFacetState(), shardStateMap);
    assertSame(shardStateMap, indexState.getShards());
  }

  @Test
  public void testGetShard() throws IOException {
    Map<Integer, ShardState> shardStateMap = new HashMap<>();
    ShardState mockShard = mock(ShardState.class);
    shardStateMap.put(0, mockShard);
    ImmutableIndexState indexState =
        getIndexState(getEmptyState(), new FieldAndFacetState(), shardStateMap);
    assertSame(mockShard, indexState.getShard(0));
  }

  @Test
  public void testGetShardNotFound() throws IOException {
    Map<Integer, ShardState> shardStateMap = new HashMap<>();
    ImmutableIndexState indexState =
        getIndexState(getEmptyState(), new FieldAndFacetState(), shardStateMap);
    try {
      indexState.getShard(1);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("shardOrd=1 does not exist in index \"test_index\"", e.getMessage());
    }
  }

  @Test
  public void testCreatesShards() throws IOException {
    ImmutableIndexState indexState = getIndexState(getEmptyState(), new FieldAndFacetState(), null);
    assertNotNull(indexState.getShard(0));
  }

  @Test
  public void testClose() throws IOException {
    Map<Integer, ShardState> shardStateMap = new HashMap<>();
    ShardState mockShard = mock(ShardState.class);
    shardStateMap.put(0, mockShard);
    ImmutableIndexState indexState =
        getIndexState(getEmptyState(), new FieldAndFacetState(), shardStateMap);
    indexState.close();
    verify(mockShard, times(1)).close();
  }

  @Test
  public void testIsStarted() throws IOException {
    Map<Integer, ShardState> shardStateMap = new HashMap<>();
    ShardState mockShard = mock(ShardState.class);
    when(mockShard.isStarted()).thenReturn(true);
    shardStateMap.put(0, mockShard);
    ImmutableIndexState indexState =
        getIndexState(getEmptyState(), new FieldAndFacetState(), shardStateMap);
    assertTrue(indexState.isStarted());
  }

  @Test
  public void testIsNotStarted() throws IOException {
    Map<Integer, ShardState> shardStateMap = new HashMap<>();
    ShardState mockShard = mock(ShardState.class);
    when(mockShard.isStarted()).thenReturn(false);
    shardStateMap.put(0, mockShard);
    ImmutableIndexState indexState =
        getIndexState(getEmptyState(), new FieldAndFacetState(), shardStateMap);
    assertFalse(indexState.isStarted());
  }

  @Test
  public void testCommit() throws IOException {
    Map<Integer, ShardState> shardStateMap = new HashMap<>();
    ShardState mockShard = mock(ShardState.class);
    when(mockShard.isPrimary()).thenReturn(false);
    shardStateMap.put(0, mockShard);
    ImmutableIndexState indexState =
        getIndexState(getEmptyState(), new FieldAndFacetState(), shardStateMap);
    indexState.commit();
    verify(mockShard, times(1)).commit();
  }

  @Test
  public void testDeleteIndex() throws IOException {
    ImmutableIndexState indexState = getIndexState(getEmptyState());
    assertEquals(1, folder.getRoot().listFiles().length);
    indexState.deleteIndex();
    assertEquals(0, folder.getRoot().listFiles().length);
  }

  @Test
  public void testMergeIndexSortChange() {
    IndexSettings settings1 =
        IndexSettings.newBuilder()
            .setIndexSort(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("field1").setReverse(true).build())
                    .build())
            .build();
    IndexSettings settings2 =
        IndexSettings.newBuilder()
            .setIndexSort(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName("field1").build())
                    .build())
            .build();
    try {
      ImmutableIndexState.mergeSettings(settings1, settings2);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Cannot change index sort value once set", e.getMessage());
    }
  }

  @Test
  public void testMergeIndexSortSame() {
    SortFields indexSort =
        SortFields.newBuilder()
            .addSortedFields(SortType.newBuilder().setFieldName("field1").setReverse(true).build())
            .build();
    IndexSettings settings1 = IndexSettings.newBuilder().setIndexSort(indexSort).build();
    IndexSettings settings2 = IndexSettings.newBuilder().setIndexSort(indexSort).build();
    IndexSettings mergedSettings = ImmutableIndexState.mergeSettings(settings1, settings2);
    assertEquals(indexSort, mergedSettings.getIndexSort());
  }

  @Test
  public void testGetSaveState_empty() throws IOException {
    ImmutableIndexState indexState = getIndexState(getEmptyState());
    JsonObject saveStateJson = indexState.getSaveState();
    assertEquals(6, saveStateJson.size());
    JsonElement element = saveStateJson.get("indexName");
    assertNotNull(element);
    assertEquals("test_index", element.getAsString());

    element = saveStateJson.get("gen");
    assertNotNull(element);
    assertEquals(1L, element.getAsLong());

    element = saveStateJson.get("committed");
    assertNotNull(element);
    assertTrue(element.getAsBoolean());

    element = saveStateJson.get("settings");
    assertNotNull(element);
    assertTrue(element.isJsonObject());
    assertEquals(0, element.getAsJsonObject().size());

    element = saveStateJson.get("liveSettings");
    assertNotNull(element);
    assertTrue(element.isJsonObject());
    assertEquals(0, element.getAsJsonObject().size());

    element = saveStateJson.get("fields");
    assertNotNull(element);
    assertTrue(element.isJsonObject());
    assertEquals(0, element.getAsJsonObject().size());
  }

  @Test
  public void testGetSaveState_settings() throws IOException {
    IndexSettings settings =
        IndexSettings.newBuilder()
            .setConcurrentMergeSchedulerMaxMergeCount(wrap(5))
            .setConcurrentMergeSchedulerMaxThreadCount(wrap(2))
            .setNrtCachingDirectoryMaxSizeMB(wrap(100.0))
            .build();
    ImmutableIndexState indexState = getIndexState(getStateWithSettings(settings));
    JsonObject saveStateJson = indexState.getSaveState();
    assertEquals(6, saveStateJson.size());
    JsonElement element = saveStateJson.get("settings");
    assertNotNull(element);
    assertTrue(element.isJsonObject());

    JsonObject jsonObject = element.getAsJsonObject();
    assertEquals(3, jsonObject.size());

    element = jsonObject.get("nrtCachingDirectoryMaxSizeMB");
    assertNotNull(element);
    assertEquals(100.0, element.getAsDouble(), 0.0);

    element = jsonObject.get("concurrentMergeSchedulerMaxThreadCount");
    assertNotNull(element);
    assertEquals(2, element.getAsInt());

    element = jsonObject.get("concurrentMergeSchedulerMaxMergeCount");
    assertNotNull(element);
    assertEquals(5, element.getAsInt());
  }

  @Test
  public void testGetSaveState_liveSettings() throws IOException {
    IndexLiveSettings liveSettings =
        IndexLiveSettings.newBuilder()
            .setDefaultTerminateAfter(wrap(100))
            .setMaxRefreshSec(wrap(10.0))
            .setSegmentsPerTier(wrap(5))
            .build();
    ImmutableIndexState indexState = getIndexState(getStateWithLiveSettings(liveSettings));
    JsonObject saveStateJson = indexState.getSaveState();
    assertEquals(6, saveStateJson.size());
    JsonElement element = saveStateJson.get("liveSettings");
    assertNotNull(element);
    assertTrue(element.isJsonObject());

    JsonObject jsonObject = element.getAsJsonObject();
    assertEquals(3, jsonObject.size());

    element = jsonObject.get("maxRefreshSec");
    assertNotNull(element);
    assertEquals(10.0, element.getAsDouble(), 0.0);

    element = jsonObject.get("segmentsPerTier");
    assertNotNull(element);
    assertEquals(5, element.getAsInt());

    element = jsonObject.get("defaultTerminateAfter");
    assertNotNull(element);
    assertEquals(100, element.getAsInt());
  }

  @Test
  public void testGetSaveState_fields() throws IOException {
    IndexStateInfo stateInfo =
        getEmptyState().toBuilder()
            .putFields(
                "field1",
                Field.newBuilder()
                    .setName("field1")
                    .setType(FieldType.ATOM)
                    .setSearch(true)
                    .setStore(false)
                    .setStoreDocValues(false)
                    .build())
            .putFields(
                "field2",
                Field.newBuilder()
                    .setName("field2")
                    .setType(FieldType.INT)
                    .setStoreDocValues(true)
                    .build())
            .build();
    ImmutableIndexState indexState = getIndexState(stateInfo);
    JsonObject saveStateJson = indexState.getSaveState();
    assertEquals(6, saveStateJson.size());
    JsonElement element = saveStateJson.get("fields");
    assertNotNull(element);
    assertTrue(element.isJsonObject());

    JsonObject jsonObject = element.getAsJsonObject();
    assertEquals(2, jsonObject.size());

    element = jsonObject.get("field1");
    assertNotNull(element);
    assertTrue(element.isJsonObject());
    JsonObject fieldObject = element.getAsJsonObject();
    assertEquals(2, fieldObject.size());

    element = fieldObject.get("name");
    assertNotNull(element);
    assertEquals("field1", element.getAsString());

    element = fieldObject.get("search");
    assertNotNull(element);
    assertTrue(element.getAsBoolean());

    element = jsonObject.get("field2");
    assertNotNull(element);
    assertTrue(element.isJsonObject());
    fieldObject = element.getAsJsonObject();
    assertEquals(3, fieldObject.size());

    element = fieldObject.get("name");
    assertNotNull(element);
    assertEquals("field2", element.getAsString());

    element = fieldObject.get("type");
    assertNotNull(element);
    assertEquals("INT", element.getAsString());

    element = fieldObject.get("storeDocValues");
    assertNotNull(element);
    assertTrue(element.getAsBoolean());
  }

  @Test
  public void testStart_notCommittedNoRestore() throws IOException {
    ShardState mockShardState = mock(ShardState.class);
    when(mockShardState.isStarted()).thenReturn(false);
    ImmutableIndexState indexState =
        getIndexState(
            IndexStateInfo.newBuilder().build(),
            new FieldAndFacetState(),
            Map.of(0, mockShardState));
    NrtDataManager mockNrtDataManager = mock(NrtDataManager.class);
    when(mockNrtDataManager.hasRestoreData()).thenReturn(false);
    ReplicationServerClient mockReplicationServerClient = mock(ReplicationServerClient.class);

    indexState.start(Mode.STANDALONE, mockNrtDataManager, -1, mockReplicationServerClient);

    verify(mockShardState, times(1)).isStarted();
    verify(mockNrtDataManager, times(1)).hasRestoreData();
    verify(mockShardState, times(1)).setDoCreate(true);
    verify(mockShardState, times(1)).start(mockNrtDataManager);
    verifyNoMoreInteractions(mockNrtDataManager, mockShardState, mockReplicationServerClient);
  }

  @Test
  public void testStart_notCommittedHasRestore() throws IOException {
    ShardState mockShardState = mock(ShardState.class);
    when(mockShardState.isStarted()).thenReturn(false);
    ImmutableIndexState indexState =
        getIndexState(
            IndexStateInfo.newBuilder().build(),
            new FieldAndFacetState(),
            Map.of(0, mockShardState));
    NrtDataManager mockNrtDataManager = mock(NrtDataManager.class);
    when(mockNrtDataManager.hasRestoreData()).thenReturn(true);
    ReplicationServerClient mockReplicationServerClient = mock(ReplicationServerClient.class);

    indexState.start(Mode.STANDALONE, mockNrtDataManager, -1, mockReplicationServerClient);

    verify(mockShardState, times(1)).isStarted();
    verify(mockNrtDataManager, times(1)).hasRestoreData();
    verify(mockShardState, times(1)).setDoCreate(false);
    verify(mockShardState, times(1)).start(mockNrtDataManager);
    verifyNoMoreInteractions(mockNrtDataManager, mockShardState, mockReplicationServerClient);
  }

  @Test
  public void testStart_committedNoRestore() throws IOException {
    ShardState mockShardState = mock(ShardState.class);
    when(mockShardState.isStarted()).thenReturn(false);
    ImmutableIndexState indexState =
        getIndexState(
            IndexStateInfo.newBuilder().setCommitted(true).build(),
            new FieldAndFacetState(),
            Map.of(0, mockShardState));
    NrtDataManager mockNrtDataManager = mock(NrtDataManager.class);
    when(mockNrtDataManager.hasRestoreData()).thenReturn(false);
    ReplicationServerClient mockReplicationServerClient = mock(ReplicationServerClient.class);

    indexState.start(Mode.STANDALONE, mockNrtDataManager, -1, mockReplicationServerClient);

    verify(mockShardState, times(1)).isStarted();
    verify(mockShardState, times(1)).setDoCreate(false);
    verify(mockShardState, times(1)).start(mockNrtDataManager);
    verifyNoMoreInteractions(mockNrtDataManager, mockShardState, mockReplicationServerClient);
  }

  @Test
  public void testStart_committedHasRestore() throws IOException {
    ShardState mockShardState = mock(ShardState.class);
    when(mockShardState.isStarted()).thenReturn(false);
    ImmutableIndexState indexState =
        getIndexState(
            IndexStateInfo.newBuilder().setCommitted(true).build(),
            new FieldAndFacetState(),
            Map.of(0, mockShardState));
    NrtDataManager mockNrtDataManager = mock(NrtDataManager.class);
    when(mockNrtDataManager.hasRestoreData()).thenReturn(true);
    ReplicationServerClient mockReplicationServerClient = mock(ReplicationServerClient.class);

    indexState.start(Mode.STANDALONE, mockNrtDataManager, -1, mockReplicationServerClient);

    verify(mockShardState, times(1)).isStarted();
    verify(mockShardState, times(1)).setDoCreate(false);
    verify(mockShardState, times(1)).start(mockNrtDataManager);
    verifyNoMoreInteractions(mockNrtDataManager, mockShardState, mockReplicationServerClient);
  }

  @Test
  public void testStart_primary() throws IOException {
    ShardState mockShardState = mock(ShardState.class);
    when(mockShardState.isStarted()).thenReturn(false);
    ImmutableIndexState indexState =
        getIndexState(
            IndexStateInfo.newBuilder().build(),
            new FieldAndFacetState(),
            Map.of(0, mockShardState));
    NrtDataManager mockNrtDataManager = mock(NrtDataManager.class);
    when(mockNrtDataManager.hasRestoreData()).thenReturn(true);
    ReplicationServerClient mockReplicationServerClient = mock(ReplicationServerClient.class);

    indexState.start(Mode.PRIMARY, mockNrtDataManager, -1, mockReplicationServerClient);

    verify(mockShardState, times(1)).isStarted();
    verify(mockNrtDataManager, times(1)).hasRestoreData();
    verify(mockShardState, times(1)).setDoCreate(false);
    verify(mockShardState, times(1)).startPrimary(mockNrtDataManager, -1);
    verifyNoMoreInteractions(mockNrtDataManager, mockShardState, mockReplicationServerClient);
  }

  @Test
  public void testStart_replica() throws IOException {
    ShardState mockShardState = mock(ShardState.class);
    when(mockShardState.isStarted()).thenReturn(false);
    ImmutableIndexState indexState =
        getIndexState(
            IndexStateInfo.newBuilder().build(),
            new FieldAndFacetState(),
            Map.of(0, mockShardState));
    NrtDataManager mockNrtDataManager = mock(NrtDataManager.class);
    when(mockNrtDataManager.hasRestoreData()).thenReturn(true);
    ReplicationServerClient mockReplicationServerClient = mock(ReplicationServerClient.class);

    indexState.start(Mode.REPLICA, mockNrtDataManager, -1, mockReplicationServerClient);

    verify(mockShardState, times(1)).isStarted();
    verify(mockNrtDataManager, times(1)).hasRestoreData();
    verify(mockShardState, times(1)).setDoCreate(false);
    verify(mockShardState, times(1))
        .startReplica(mockNrtDataManager, mockReplicationServerClient, -1);
    verifyNoMoreInteractions(mockNrtDataManager, mockShardState, mockReplicationServerClient);
  }

  @Test
  public void testStart_alreadyStarted() throws IOException {
    ShardState mockShardState = mock(ShardState.class);
    when(mockShardState.isStarted()).thenReturn(true);
    ImmutableIndexState indexState =
        getIndexState(
            IndexStateInfo.newBuilder().build(),
            new FieldAndFacetState(),
            Map.of(0, mockShardState));
    NrtDataManager mockNrtDataManager = mock(NrtDataManager.class);
    ReplicationServerClient mockReplicationServerClient = mock(ReplicationServerClient.class);

    try {
      indexState.start(Mode.STANDALONE, mockNrtDataManager, -1, mockReplicationServerClient);
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Index \"test_index\" is already started", e.getMessage());
    }

    verify(mockShardState, times(1)).isStarted();
    verifyNoMoreInteractions(mockNrtDataManager, mockShardState, mockReplicationServerClient);
  }
}
