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
package com.yelp.nrtsearch.server.luceneserver.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.gson.JsonObject;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.LiveSettingsRequest;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.LiveSettingsHandler;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefCreator;
import com.yelp.nrtsearch.server.luceneserver.similarity.SimilarityCreator;
import java.io.IOException;
import java.util.Collections;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LegacyIndexStateTest {

  @ClassRule public static final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testDefaultSliceParams() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      assertEquals(LegacyIndexState.DEFAULT_SLICE_MAX_DOCS, indexState.getSliceMaxDocs());
      assertEquals(LegacyIndexState.DEFAULT_SLICE_MAX_SEGMENTS, indexState.getSliceMaxSegments());
    }
  }

  @Test
  public void testDefaultVirtualShards() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      assertEquals(1, indexState.getVirtualShards());
    }
  }

  @Test
  public void testDefaultMaxMergedSegmentMB() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      assertEquals(0, indexState.getMaxMergedSegmentMB());
    }
  }

  @Test
  public void testDefaultSegmentsPerTier() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      assertEquals(0, indexState.getSegmentsPerTier());
    }
  }

  @Test
  public void testDefaultSearchTimeout() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      assertEquals(0, indexState.getDefaultSearchTimeoutSec(), 0);
    }
  }

  @Test
  public void testDefaultTimeoutCheckEvery() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      assertEquals(0, indexState.getDefaultSearchTimeoutCheckEvery());
    }
  }

  @Test
  public void testDefaultTerminateAfter() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      assertEquals(0, indexState.getDefaultTerminateAfter());
    }
  }

  @Test
  public void testDefaultRefreshSec() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      assertEquals(0.05f, indexState.minRefreshSec, Math.ulp(0.05f));
      assertEquals(1.0f, indexState.maxRefreshSec, Math.ulp(1.0f));
    }
  }

  @Test
  public void testChangeSliceParams() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setSliceMaxDocs(100);
      indexState.setSliceMaxSegments(50);
      assertEquals(100, indexState.getSliceMaxDocs());
      assertEquals(50, indexState.getSliceMaxSegments());
    }
  }

  @Test
  public void testChangeVirtualShards() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setVirtualShards(10);
      assertEquals(10, indexState.getVirtualShards());
    }
  }

  @Test
  public void testChangeMaxMergedSegmentMB() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setMaxMergedSegmentMB(500);
      assertEquals(500, indexState.getMaxMergedSegmentMB());
    }
  }

  @Test
  public void testChangeSegmentsPerTier() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setSegmentsPerTier(5);
      assertEquals(5, indexState.getSegmentsPerTier());
    }
  }

  @Test
  public void testChangeSearchTimeout() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setDefaultSearchTimeoutSec(2.0);
      assertEquals(2.0, indexState.getDefaultSearchTimeoutSec(), 0);
    }
  }

  @Test
  public void testChangeCheckEvery() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setDefaultSearchTimeoutCheckEvery(10);
      assertEquals(10, indexState.getDefaultSearchTimeoutCheckEvery());
    }
  }

  @Test
  public void testChangeDefaultTerminateAfter() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setDefaultTerminateAfter(11);
      assertEquals(11, indexState.getDefaultTerminateAfter());
    }
  }

  @Test
  public void testChangeRefreshSec() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setRefreshSec(2.0, 3.0);
      assertEquals(2.0, indexState.minRefreshSec, 0);
      assertEquals(3.0, indexState.maxRefreshSec, 0);
    }
  }

  @Test
  public void testDisabledVirtualShards() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setVirtualShards(10);
      assertEquals(1, indexState.getVirtualShards());
    }
  }

  @Test
  public void testInvalidSliceDocs() throws IOException {
    String expectedMessage = "Max slice docs must be greater than 0.";
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      try {
        indexState.setSliceMaxDocs(0);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
      try {
        indexState.setSliceMaxDocs(-1);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
    }
  }

  @Test
  public void testInvalidSliceSegments() throws IOException {
    String expectedMessage = "Max slice segments must be greater than 0.";
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      try {
        indexState.setSliceMaxSegments(0);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
      try {
        indexState.setSliceMaxSegments(-1);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
    }
  }

  @Test
  public void testInvalidVirtualShards() throws IOException {
    String expectedMessage = "Number of virtual shards must be greater than 0.";
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      try {
        indexState.setVirtualShards(0);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
      try {
        indexState.setVirtualShards(-1);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
    }
  }

  @Test
  public void testInvalidMaxMergedSegmentMB() throws IOException {
    String expectedMessage = "Max merged segment size must be greater than 0.";
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      try {
        indexState.setMaxMergedSegmentMB(0);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
      try {
        indexState.setMaxMergedSegmentMB(-1);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
    }
  }

  @Test
  public void testInvalidSegmentsPerTier() throws IOException {
    String expectedMessage = "Segments per tier must be >= 2.";
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      try {
        indexState.setSegmentsPerTier(1);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
      try {
        indexState.setSegmentsPerTier(-1);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
    }
  }

  @Test
  public void testInvalidSearchTimeout() throws IOException {
    String expectedMessage = "Default search timeout must be >= 0.";
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      try {
        indexState.setDefaultSearchTimeoutSec(-1);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
    }
  }

  @Test
  public void testInvalidTimeoutCheckEvery() throws IOException {
    String expectedMessage = "Default search timeout check every must be >= 0.";
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      try {
        indexState.setDefaultSearchTimeoutCheckEvery(-1);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
    }
  }

  @Test
  public void testInvalidDefaultTerminateAfter() throws IOException {
    String expectedMessage = "Default terminate after must be >= 0.";
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      try {
        indexState.setDefaultTerminateAfter(-1);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
    }
  }

  @Test
  public void testInvalidRefreshSec() throws IOException {
    String expectedMessage = "Min and Max refresh seconds must be > 0";
    String expectedMessage2 = "Max refresh seconds must be >= Min refresh seconds";
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      try {
        indexState.setRefreshSec(1.0, 0.0);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
      try {
        indexState.setRefreshSec(0.0, 2.0);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage, e.getMessage());
      }
      try {
        indexState.setRefreshSec(3.0, 2.0);
        fail();
      } catch (IllegalArgumentException e) {
        assertEquals(expectedMessage2, e.getMessage());
      }
    }
  }

  @Test
  public void testSliceParamsLoad() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setSliceMaxDocs(200);
      indexState.setSliceMaxSegments(75);

      JsonObject saveState = indexState.getSaveState();
      LiveSettingsRequest liveSettingsRequest =
          indexState.buildLiveSettingsRequest(saveState.get("liveSettings").toString());
      assertEquals(200, liveSettingsRequest.getSliceMaxDocs());
      assertEquals(75, liveSettingsRequest.getSliceMaxSegments());
    }
  }

  @Test
  public void testVirtualShardsLoad() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setVirtualShards(20);

      JsonObject saveState = indexState.getSaveState();
      LiveSettingsRequest liveSettingsRequest =
          indexState.buildLiveSettingsRequest(saveState.get("liveSettings").toString());
      assertEquals(20, liveSettingsRequest.getVirtualShards());
    }
  }

  @Test
  public void testMaxMergedSegmentMBLoad() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setMaxMergedSegmentMB(100);

      JsonObject saveState = indexState.getSaveState();
      LiveSettingsRequest liveSettingsRequest =
          indexState.buildLiveSettingsRequest(saveState.get("liveSettings").toString());
      assertEquals(100, liveSettingsRequest.getMaxMergedSegmentMB());
    }
  }

  @Test
  public void testSegmentsPerTierLoad() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setSegmentsPerTier(4);

      JsonObject saveState = indexState.getSaveState();
      LiveSettingsRequest liveSettingsRequest =
          indexState.buildLiveSettingsRequest(saveState.get("liveSettings").toString());
      assertEquals(4, liveSettingsRequest.getSegmentsPerTier());
    }
  }

  @Test
  public void testSearchTimeoutLoad() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setDefaultSearchTimeoutSec(4.0);

      JsonObject saveState = indexState.getSaveState();
      LiveSettingsRequest liveSettingsRequest =
          indexState.buildLiveSettingsRequest(saveState.get("liveSettings").toString());
      assertEquals(4.0, liveSettingsRequest.getDefaultSearchTimeoutSec(), 0);
    }
  }

  @Test
  public void testTimeoutCheckEveryLoad() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setDefaultSearchTimeoutCheckEvery(25);

      JsonObject saveState = indexState.getSaveState();
      LiveSettingsRequest liveSettingsRequest =
          indexState.buildLiveSettingsRequest(saveState.get("liveSettings").toString());
      assertEquals(25, liveSettingsRequest.getDefaultSearchTimeoutCheckEvery());
    }
  }

  @Test
  public void testDefaultTerminateAfterLoad() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setDefaultTerminateAfter(50);

      JsonObject saveState = indexState.getSaveState();
      LiveSettingsRequest liveSettingsRequest =
          indexState.buildLiveSettingsRequest(saveState.get("liveSettings").toString());
      assertEquals(50, liveSettingsRequest.getDefaultTerminateAfter());
    }
  }

  @Test
  public void testRefreshSecLoad() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      indexState.setRefreshSec(5.0, 7.0);

      JsonObject saveState = indexState.getSaveState();
      LiveSettingsRequest liveSettingsRequest =
          indexState.buildLiveSettingsRequest(saveState.get("liveSettings").toString());
      assertEquals(5.0, liveSettingsRequest.getMinRefreshSec(), 0);
      assertEquals(7.0, liveSettingsRequest.getMaxRefreshSec(), 0);
    }
  }

  @Test
  public void testSliceParamsSetByLiveSettingsHandler() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest =
          LiveSettingsRequest.newBuilder().setSliceMaxDocs(300).setSliceMaxSegments(150).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(300, indexState.getSliceMaxDocs());
      assertEquals(150, indexState.getSliceMaxSegments());
    }
  }

  @Test
  public void testVirtualShardsSetByLiveSettingsHandler() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest =
          LiveSettingsRequest.newBuilder().setVirtualShards(30).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(30, indexState.getVirtualShards());
    }
  }

  @Test
  public void testMaxMergedSegmentMBSetByLiveSettingsHandler() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest =
          LiveSettingsRequest.newBuilder().setMaxMergedSegmentMB(100).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(100, indexState.getMaxMergedSegmentMB());
    }
  }

  @Test
  public void testSegmentsPerTierSetByLiveSettingsHandler() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest =
          LiveSettingsRequest.newBuilder().setSegmentsPerTier(11).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(11, indexState.getSegmentsPerTier());
    }
  }

  @Test
  public void testSearchTimeoutSetByLiveSettingsHandler() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest =
          LiveSettingsRequest.newBuilder().setDefaultSearchTimeoutSec(10.0).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(10.0, indexState.getDefaultSearchTimeoutSec(), 0);
    }
  }

  @Test
  public void testTimeoutCheckEverySetByLiveSettingsHandler() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest =
          LiveSettingsRequest.newBuilder().setDefaultSearchTimeoutCheckEvery(50).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(50, indexState.getDefaultSearchTimeoutCheckEvery());
    }
  }

  @Test
  public void testDefaultTerminateAfterSetByLiveSettingsHandler() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest =
          LiveSettingsRequest.newBuilder().setDefaultTerminateAfter(60).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(60, indexState.getDefaultTerminateAfter());
    }
  }

  @Test
  public void testRefreshSecSetByLiveSettingsHandler() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest =
          LiveSettingsRequest.newBuilder().setMaxRefreshSec(4.0).setMinRefreshSec(3.0).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(3.0, indexState.minRefreshSec, 0);
      assertEquals(4.0, indexState.maxRefreshSec, 0);

      liveSettingsRequest = LiveSettingsRequest.newBuilder().setMinRefreshSec(2.0).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(2.0, indexState.minRefreshSec, 0);
      assertEquals(4.0, indexState.maxRefreshSec, 0);

      liveSettingsRequest = LiveSettingsRequest.newBuilder().setMaxRefreshSec(5.0).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(2.0, indexState.minRefreshSec, 0);
      assertEquals(5.0, indexState.maxRefreshSec, 0);
    }
  }

  @Test
  public void testSliceParamsLiveSettingsHandlerNoop() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest = LiveSettingsRequest.newBuilder().build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(LegacyIndexState.DEFAULT_SLICE_MAX_DOCS, indexState.getSliceMaxDocs());
      assertEquals(LegacyIndexState.DEFAULT_SLICE_MAX_SEGMENTS, indexState.getSliceMaxSegments());
    }
  }

  @Test
  public void testVirtualShardsLiveSettingsHandlerNoop() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest = LiveSettingsRequest.newBuilder().build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(1, indexState.getVirtualShards());
    }
  }

  @Test
  public void testMaxMergedSegmentMBLiveSettingsHandlerNoop() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest = LiveSettingsRequest.newBuilder().build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(0, indexState.getMaxMergedSegmentMB());
    }
  }

  @Test
  public void testSegmentsPerTierLiveSettingsHandlerNoop() throws IOException {
    try (GlobalState globalState = getInitStateVirtualSharding()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest = LiveSettingsRequest.newBuilder().build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(0, indexState.getSegmentsPerTier());
    }
  }

  @Test
  public void testSearchTimeoutLiveSettingsHandlerNoop() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest =
          LiveSettingsRequest.newBuilder().setDefaultSearchTimeoutSec(-1).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(0.0, indexState.getDefaultSearchTimeoutSec(), 0);
    }
  }

  @Test
  public void testTimeoutCheckEveryLiveSettingsHandlerNoop() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest =
          LiveSettingsRequest.newBuilder().setDefaultSearchTimeoutCheckEvery(-1).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(0.0, indexState.getDefaultSearchTimeoutCheckEvery(), 0);
    }
  }

  @Test
  public void testDefaultTerminateAfterLiveSettingsHandlerNoop() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest =
          LiveSettingsRequest.newBuilder().setDefaultTerminateAfter(-1).build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(0, indexState.getDefaultTerminateAfter());
    }
  }

  @Test
  public void testRefreshSecLiveSettingsHandlerNoop() throws IOException {
    try (GlobalState globalState = getInitState()) {
      LegacyIndexState indexState = new LegacyIndexState(globalState, "testIdx", null, true, false);
      LiveSettingsRequest liveSettingsRequest = LiveSettingsRequest.newBuilder().build();
      new LiveSettingsHandler().handle(indexState, liveSettingsRequest);

      assertEquals(0.05f, indexState.minRefreshSec, Math.ulp(0.05f));
      assertEquals(1.0f, indexState.maxRefreshSec, Math.ulp(1.0f));
    }
  }

  public GlobalState getInitState() throws IOException {
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    FieldDefCreator.initialize(luceneServerConfiguration, Collections.emptyList());
    SimilarityCreator.initialize(luceneServerConfiguration, Collections.emptyList());
    return GlobalState.createState(luceneServerConfiguration);
  }

  public GlobalState getInitStateVirtualSharding() throws IOException {
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(
            Mode.STANDALONE, folder.getRoot(), "virtualSharding: true");
    FieldDefCreator.initialize(luceneServerConfiguration, Collections.emptyList());
    SimilarityCreator.initialize(luceneServerConfiguration, Collections.emptyList());
    return GlobalState.createState(luceneServerConfiguration);
  }
}
