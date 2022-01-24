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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.ClassRule;
import org.junit.Test;

public class DeadLineUtilsEnableTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public String getExtraConfig() {
    return "deadlineCancellation: true";
  }

  @Test
  public void testDeadlineCancellationEnabled() {
    assertTrue(DeadlineUtils.getCancellationEnabled());
  }
}
