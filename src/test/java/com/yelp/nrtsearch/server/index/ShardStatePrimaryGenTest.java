/*
 * Copyright 2026 Yelp Inc.
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

import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class ShardStatePrimaryGenTest {

  @Test
  public void testNotEnforced() {
    ShardState.verifyPrimaryGenOrdering(1, 5, false);
    ShardState.verifyPrimaryGenOrdering(5, 5, false);
  }

  @Test
  public void testNoRestoredGen() {
    ShardState.verifyPrimaryGenOrdering(1, -1, true);
  }

  @Test
  public void testNewerGen() {
    ShardState.verifyPrimaryGenOrdering(6, 5, true);
  }

  @Test
  public void testSameOrOlderGenRejected() {
    assertThrows(
        IllegalStateException.class, () -> ShardState.verifyPrimaryGenOrdering(5, 5, true));
    assertThrows(
        IllegalStateException.class, () -> ShardState.verifyPrimaryGenOrdering(4, 5, true));
  }
}
