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
package com.yelp.nrtsearch.server.remote;

import static org.junit.Assert.assertArrayEquals;

import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointStateTest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class RemoteUtilsTest {

  @Test
  public void testPointStateToUtf8() throws IOException {
    byte[] result = RemoteUtils.pointStateToUtf8(NrtPointStateTest.getNrtPointState());
    byte[] expected = NrtPointStateTest.getExpectedJson().getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(expected, result);
  }

  @Test
  public void testPointStateFromUtf8() throws IOException {
    byte[] data = NrtPointStateTest.getExpectedJson().getBytes(StandardCharsets.UTF_8);
    NrtPointStateTest.assertNrtPointState(RemoteUtils.pointStateFromUtf8(data));
  }
}
