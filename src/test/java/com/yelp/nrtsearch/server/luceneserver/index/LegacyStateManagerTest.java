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
package com.yelp.nrtsearch.server.luceneserver.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import java.io.IOException;
import org.junit.Test;

public class LegacyStateManagerTest {

  @FunctionalInterface
  private interface MyRunnable {
    void run() throws IOException;
  }

  @Test
  public void testGetCurrent() {
    IndexState mockState = mock(IndexState.class);
    LegacyStateManager manager = new LegacyStateManager(mockState);
    assertSame(mockState, manager.getCurrent());
  }

  @Test
  public void testUnsupported() throws IOException {
    IndexState mockState = mock(IndexState.class);
    LegacyStateManager manager = new LegacyStateManager(mockState);
    testFunc(manager::load);
    testFunc(manager::create);
    testFunc(manager::getSettings);
    testFunc(() -> manager.updateSettings(null));
    testFunc(manager::getLiveSettings);
    testFunc(() -> manager.updateLiveSettings(null));
    testFunc(() -> manager.updateFields(null));
    testFunc(() -> manager.start(Mode.PRIMARY, null, 1, null));
  }

  private void testFunc(MyRunnable func) throws IOException {
    try {
      func.run();
      fail();
    } catch (UnsupportedOperationException e) {
      assertEquals(LegacyStateManager.EXCEPTION_MSG, e.getMessage());
    }
  }
}
