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
package com.yelp.nrtsearch.tools.nrt_utils.backup;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.utils.JsonUtils;
import java.io.IOException;
import org.junit.Test;

public class SnapshotMetadataTest {

  @Test
  public void testSnapshotMetadata() {
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata("serviceName", "indexName", "timeStringMs", 1000);
    assertEquals("serviceName", snapshotMetadata.getServiceName());
    assertEquals("indexName", snapshotMetadata.getIndexName());
    assertEquals("timeStringMs", snapshotMetadata.getTimeStringMs());
    assertEquals(1000, snapshotMetadata.getIndexSizeBytes());
  }

  @Test
  public void testToJson() throws IOException {
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata("serviceName", "indexName", "timeStringMs", 1000);
    String json = JsonUtils.objectToJsonStr(snapshotMetadata);
    assertEquals(
        "{\"serviceName\":\"serviceName\",\"indexName\":\"indexName\",\"timeStringMs\":\"timeStringMs\",\"indexSizeBytes\":1000}",
        json);
  }

  @Test
  public void testFromJson() throws IOException {
    String json =
        "{\"serviceName\":\"serviceName\",\"indexName\":\"indexName\",\"timeStringMs\":\"timeStringMs\",\"indexSizeBytes\":1000}";
    SnapshotMetadata snapshotMetadata = JsonUtils.readValue(json, SnapshotMetadata.class);
    assertEquals("serviceName", snapshotMetadata.getServiceName());
    assertEquals("indexName", snapshotMetadata.getIndexName());
    assertEquals("timeStringMs", snapshotMetadata.getTimeStringMs());
    assertEquals(1000, snapshotMetadata.getIndexSizeBytes());
  }

  @Test
  public void testFromJson_unknownFields() throws IOException {
    String json =
        "{\"serviceName\":\"serviceName\",\"indexName\":\"indexName\",\"timeStringMs\":\"timeStringMs\",\"indexSizeBytes\":1000,\"unknownField\":\"unknownValue\"}";
    SnapshotMetadata snapshotMetadata = JsonUtils.readValue(json, SnapshotMetadata.class);
    assertEquals("serviceName", snapshotMetadata.getServiceName());
    assertEquals("indexName", snapshotMetadata.getIndexName());
    assertEquals("timeStringMs", snapshotMetadata.getTimeStringMs());
    assertEquals(1000, snapshotMetadata.getIndexSizeBytes());
  }
}
