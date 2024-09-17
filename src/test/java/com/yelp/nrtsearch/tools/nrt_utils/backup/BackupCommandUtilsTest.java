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
import static org.junit.Assert.fail;

import org.junit.Test;

public class BackupCommandUtilsTest {
  @Test
  public void testGetSnapshotRoot_rootProvided() {
    assertEquals("root/", BackupCommandUtils.getSnapshotRoot("root/", null));
    assertEquals("root/", BackupCommandUtils.getSnapshotRoot("root/", "service"));
    assertEquals("root/", BackupCommandUtils.getSnapshotRoot("root", "service"));
  }

  @Test
  public void testGetSnapshotRoot_serviceProvided() {
    assertEquals(
        "service_name/snapshots/", BackupCommandUtils.getSnapshotRoot(null, "service_name"));
  }

  @Test
  public void testGetSnapshotRoot_neitherProvided() {
    try {
      BackupCommandUtils.getSnapshotRoot(null, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Must specify snapshotRoot or serviceName", e.getMessage());
    }
  }

  @Test
  public void testGetSnapshotIndexDataRoot() {
    assertEquals(
        "root/test_index/time_string/",
        BackupCommandUtils.getSnapshotIndexDataRoot("root/", "test_index", "time_string"));
  }

  @Test
  public void testGetSnapshotIndexMetadataKey() {
    assertEquals(
        "root/metadata/test_index/time_string",
        BackupCommandUtils.getSnapshotIndexMetadataKey("root/", "test_index", "time_string"));
  }

  @Test
  public void testGetSnapshotIndexMetadataPrefix() {
    assertEquals(
        "root/metadata/test_index/",
        BackupCommandUtils.getSnapshotIndexMetadataPrefix("root/", "test_index"));
  }

  @Test
  public void testGetTimeIntervalMs() {
    assertEquals(1000, BackupCommandUtils.getTimeIntervalMs("1s"));
    assertEquals(60000, BackupCommandUtils.getTimeIntervalMs("1m"));
    assertEquals(3600000, BackupCommandUtils.getTimeIntervalMs("1h"));
    assertEquals(86400000, BackupCommandUtils.getTimeIntervalMs("1d"));
  }

  @Test
  public void testGetTimeIntervalMs_invalid() {
    try {
      BackupCommandUtils.getTimeIntervalMs("1x");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Unknown time unit: x", e.getMessage());
    }
  }
}
