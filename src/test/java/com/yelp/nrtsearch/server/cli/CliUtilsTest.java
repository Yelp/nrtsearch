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
package com.yelp.nrtsearch.server.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.protobuf.InvalidProtocolBufferException;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CliUtilsTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testMergeBuilderFromParam_jsonString() throws IOException {
    IndexLiveSettings liveSettings =
        getFromParam("{\"indexRamBufferSizeMB\": 1000, \"sliceMaxSegments\": 100}");
    assertEquals(1000.0, liveSettings.getIndexRamBufferSizeMB().getValue(), 0);
    assertEquals(100, liveSettings.getSliceMaxSegments().getValue());
  }

  @Test
  public void testMergeBuilderFromParam_filePath() throws IOException {
    File messageFile = writeToFile("{\"indexRamBufferSizeMB\": 1000, \"sliceMaxSegments\": 100}");
    IndexLiveSettings liveSettings = getFromParam("@" + messageFile.getPath());
    assertEquals(1000.0, liveSettings.getIndexRamBufferSizeMB().getValue(), 0);
    assertEquals(100, liveSettings.getSliceMaxSegments().getValue());
  }

  @Test
  public void testMergeBuilderFromParam_emptyParam() throws IOException {
    try {
      getFromParam("");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Parameter cannot be empty", e.getMessage());
    }
  }

  @Test
  public void testMergeBuilderFromParam_emptyPath() throws IOException {
    try {
      getFromParam("@");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Parameter path cannot be empty", e.getMessage());
    }
  }

  @Test
  public void testMergeBuilderFromParam_invalidProtoJson() throws IOException {
    try {
      getFromParam("[\"invalid_message\"]");
      fail();
    } catch (InvalidProtocolBufferException e) {
      assertEquals("Expect message object but got: [\"invalid_message\"]", e.getMessage());
    }
  }

  @Test
  public void testMergeBuilderFromParam_invalidProtoFile() throws IOException {
    File messageFile = writeToFile("[\"invalid_message\"]");
    try {
      getFromParam("@" + messageFile.getPath());
      fail();
    } catch (InvalidProtocolBufferException e) {
      assertEquals("Expect message object but got: [\"invalid_message\"]", e.getMessage());
    }
  }

  private IndexLiveSettings getFromParam(String param) throws IOException {
    IndexLiveSettings.Builder builder = IndexLiveSettings.newBuilder();
    CliUtils.mergeBuilderFromParam(param, builder);
    return builder.build();
  }

  private File writeToFile(String s) throws IOException {
    File file = folder.newFile();
    try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
      fileOutputStream.write(s.getBytes());
    }
    return file;
  }
}
