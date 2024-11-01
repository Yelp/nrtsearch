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

import static org.junit.Assert.*;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.index.DirectoryFactory;
import java.io.ByteArrayInputStream;
import org.apache.lucene.search.suggest.document.CompletionPostingsFormat.FSTLoadMode;
import org.junit.Test;

public class NrtsearchConfigTest {

  private NrtsearchConfig getForConfig(String config) {
    return new NrtsearchConfig(new ByteArrayInputStream(config.getBytes()));
  }

  @Test
  public void testGetsHostName() {
    String config = String.join("\n", "nodeName: \"server_foo\"", "hostName: my_host_name");
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals("my_host_name", luceneConfig.getHostName());
  }

  @Test
  public void testGetEnvHostName() {
    String config = String.join("\n", "nodeName: \"server_foo\"", "hostName: ${CUSTOM_HOST}");
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals("my_custom_host", luceneConfig.getHostName());
  }

  @Test
  public void testGetMultiEnvHostName() {
    String config =
        String.join("\n", "nodeName: \"server_foo\"", "hostName: my_${VAR1}_${VAR2}_${VAR1}_host");
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals("my_v1_v2_v1_host", luceneConfig.getHostName());
  }

  @Test
  public void testMissingEnvHostName() {
    String config =
        String.join("\n", "nodeName: \"server_foo\"", "hostName: my_${VAR4}_${VAR3}_${VAR4}_host");
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals("my__v3__host", luceneConfig.getHostName());
  }

  @Test
  public void testDefaultDiscoveryFileUpdateInterval() {
    String config = "nodeName: \"server_foo\"";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(
        ReplicationServerClient.FILE_UPDATE_INTERVAL_MS,
        luceneConfig.getDiscoveryFileUpdateIntervalMs());
  }

  @Test
  public void testSetDiscoveryFileUpdateInterval() {
    String config =
        String.join("\n", "nodeName: \"server_foo\"", "discoveryFileUpdateIntervalMs: 100");
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(100, luceneConfig.getDiscoveryFileUpdateIntervalMs());
  }

  @Test
  public void testDefaultCompletionCodecLoadMode() {
    String config = "nodeName: \"server_foo\"";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(FSTLoadMode.ON_HEAP, luceneConfig.getCompletionCodecLoadMode());
  }

  @Test
  public void testSetCompletionCodecLoadMode() {
    String config = "completionCodecLoadMode: OFF_HEAP";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(FSTLoadMode.OFF_HEAP, luceneConfig.getCompletionCodecLoadMode());
  }

  @Test
  public void testInitialSyncPrimaryWaitMs_default() {
    String config = "nodeName: \"server_foo\"";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(
        NrtsearchConfig.DEFAULT_INITIAL_SYNC_PRIMARY_WAIT_MS,
        luceneConfig.getInitialSyncPrimaryWaitMs());
  }

  @Test
  public void testInitialSyncPrimaryWaitMs_set() {
    String config = "initialSyncPrimaryWaitMs: 100";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(100L, luceneConfig.getInitialSyncPrimaryWaitMs());
  }

  @Test
  public void testInitialSyncMaxTimeMs_default() {
    String config = "nodeName: \"server_foo\"";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(
        NrtsearchConfig.DEFAULT_INITIAL_SYNC_MAX_TIME_MS, luceneConfig.getInitialSyncMaxTimeMs());
  }

  @Test
  public void testInitialSyncMaxTimeMs_set() {
    String config = "initialSyncMaxTimeMs: 100";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(100L, luceneConfig.getInitialSyncMaxTimeMs());
  }

  @Test
  public void testMaxS3ClientRetries_default() {
    String config = "nodeName: \"server_foo\"";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(
        NrtsearchConfig.DEFAULT_MAX_S3_CLIENT_RETRIES, luceneConfig.getMaxS3ClientRetries());
  }

  @Test
  public void testMaxS3ClientRetries_set() {
    String config = "maxS3ClientRetries: 10";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(10, luceneConfig.getMaxS3ClientRetries());
  }

  @Test
  public void testLiveSettingsOverride_default() {
    String config = "nodeName: \"server_foo\"";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(
        IndexLiveSettings.newBuilder().build(), luceneConfig.getLiveSettingsOverride("test_index"));
  }

  @Test
  public void testLiveSettingsOverride_set() {
    String config =
        String.join(
            "\n",
            "indexLiveSettingsOverrides:",
            "  test_index:",
            "    sliceMaxDocs: 1",
            "    virtualShards: 100",
            "  test_index_2:",
            "    defaultSearchTimeoutSec: 10.25",
            "    segmentsPerTier: 30");
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(
        IndexLiveSettings.newBuilder()
            .setSliceMaxDocs(Int32Value.newBuilder().setValue(1).build())
            .setVirtualShards(Int32Value.newBuilder().setValue(100).build())
            .build(),
        luceneConfig.getLiveSettingsOverride("test_index"));
    assertEquals(
        IndexLiveSettings.newBuilder()
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(10.25).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(30).build())
            .build(),
        luceneConfig.getLiveSettingsOverride("test_index_2"));
    assertEquals(
        IndexLiveSettings.newBuilder().build(),
        luceneConfig.getLiveSettingsOverride("test_index_3"));
  }

  @Test
  public void testLowPriorityCopyPercentage_default() {
    String config = "nodeName: \"server_foo\"";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(0, luceneConfig.getLowPriorityCopyPercentage());
  }

  @Test
  public void testLowPriorityCopyPercentage_set() {
    String config = "lowPriorityCopyPercentage: 10";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(10, luceneConfig.getLowPriorityCopyPercentage());
  }

  @Test
  public void testVerifyReplicationIndexId_default() {
    String config = "nodeName: \"server_foo\"";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertTrue(luceneConfig.getVerifyReplicationIndexId());
  }

  @Test
  public void testVerifyReplicationIndexId_set() {
    String config = "verifyReplicationIndexId: false";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertFalse(luceneConfig.getVerifyReplicationIndexId());
  }

  @Test
  public void testMMapGrouping_default() {
    String config = "nodeName: \"server_foo\"";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(DirectoryFactory.MMapGrouping.SEGMENT, luceneConfig.getMMapGrouping());
  }

  @Test
  public void testMMapGrouping_set() {
    String config = "mmapGrouping: NONE";
    NrtsearchConfig luceneConfig = getForConfig(config);
    assertEquals(DirectoryFactory.MMapGrouping.NONE, luceneConfig.getMMapGrouping());
  }
}
