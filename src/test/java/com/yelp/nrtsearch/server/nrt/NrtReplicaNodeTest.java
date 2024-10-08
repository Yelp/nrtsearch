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
package com.yelp.nrtsearch.server.nrt;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class NrtReplicaNodeTest {

  @Test
  public void testDefaultCopyThread() {
    NrtCopyThread ct = NRTReplicaNode.getNrtCopyThread(null, 0);
    assertTrue(ct instanceof DefaultCopyThread);
  }

  @Test
  public void testProportionalCopyThread() {
    NrtCopyThread ct = NRTReplicaNode.getNrtCopyThread(null, 15);
    assertTrue(ct instanceof ProportionalCopyThread);
  }
}
