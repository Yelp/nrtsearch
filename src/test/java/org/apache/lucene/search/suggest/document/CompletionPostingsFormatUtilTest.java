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
package org.apache.lucene.search.suggest.document;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import java.io.IOException;
import org.apache.lucene.search.suggest.document.CompletionPostingsFormat.FSTLoadMode;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CompletionPostingsFormatUtilTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  @Test
  public void testDefaultFSTLoadMode() throws IOException {
    TestServer.builder(folder)
        .withAutoStartConfig(true, Mode.REPLICA, 0, IndexDataLocationType.LOCAL)
        .build();
    assertEquals(FSTLoadMode.ON_HEAP, CompletionPostingsFormatUtil.getCompletionCodecLoadMode());
  }

  @Test
  public void testSetFSTLoadMode() throws IOException {
    TestServer.builder(folder)
        .withAutoStartConfig(true, Mode.REPLICA, 0, IndexDataLocationType.LOCAL)
        .withAdditionalConfig("completionCodecLoadMode: OFF_HEAP")
        .build();
    assertEquals(FSTLoadMode.OFF_HEAP, CompletionPostingsFormatUtil.getCompletionCodecLoadMode());
  }
}
