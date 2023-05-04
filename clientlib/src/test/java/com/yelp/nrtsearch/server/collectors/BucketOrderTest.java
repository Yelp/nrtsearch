/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.collectors;

import static com.yelp.nrtsearch.server.collectors.BucketOrder.COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.collectors.BucketOrder.ValueType;
import com.yelp.nrtsearch.server.grpc.BucketOrder.OrderType;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.MaxCollector;
import com.yelp.nrtsearch.server.grpc.TopHitsCollector;
import java.util.Collections;
import org.junit.Test;

public class BucketOrderTest {

  @Test
  public void testUnknownCollector() {
    com.yelp.nrtsearch.server.grpc.BucketOrder grpcOrder =
        com.yelp.nrtsearch.server.grpc.BucketOrder.newBuilder().setKey("unknown").build();
    try {
      BucketOrder.createBucketOrder(grpcOrder, Collections.emptyMap());
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Nested collector not found: unknown", e.getMessage());
    }
  }

  @Test
  public void testInvalidCollector() {
    com.yelp.nrtsearch.server.grpc.BucketOrder grpcOrder =
        com.yelp.nrtsearch.server.grpc.BucketOrder.newBuilder().setKey("invalid").build();
    try {
      BucketOrder.createBucketOrder(
          grpcOrder,
          Collections.singletonMap(
              "invalid",
              Collector.newBuilder()
                  .setTopHitsCollector(TopHitsCollector.newBuilder().build())
                  .build()));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Collector cannot be used for ordering: invalid", e.getMessage());
    }
  }

  @Test
  public void testDefaultOrder() {
    BucketOrder order = BucketOrder.DEFAULT_ORDER;
    assertEquals(ValueType.COUNT, order.getValueType());
    assertEquals(OrderType.DESC, order.getOrderType());
    assertUnsupported(order::getOrderCollectorName);
    assertUnsupported(order::getCollectorResultComparator);
  }

  @Test
  public void testCountOrder() {
    com.yelp.nrtsearch.server.grpc.BucketOrder grpcOrder =
        com.yelp.nrtsearch.server.grpc.BucketOrder.newBuilder()
            .setKey(COUNT)
            .setOrder(OrderType.ASC)
            .build();
    BucketOrder order = BucketOrder.createBucketOrder(grpcOrder, Collections.emptyMap());
    assertEquals(ValueType.COUNT, order.getValueType());
    assertEquals(OrderType.ASC, order.getOrderType());
    assertUnsupported(order::getOrderCollectorName);
    assertUnsupported(order::getCollectorResultComparator);
  }

  @Test
  public void testNestedCollectorOrder() {
    com.yelp.nrtsearch.server.grpc.BucketOrder grpcOrder =
        com.yelp.nrtsearch.server.grpc.BucketOrder.newBuilder()
            .setKey("collector")
            .setOrder(OrderType.ASC)
            .build();
    BucketOrder order =
        BucketOrder.createBucketOrder(
            grpcOrder,
            Collections.singletonMap(
                "collector",
                Collector.newBuilder().setMax(MaxCollector.newBuilder().build()).build()));
    assertEquals(ValueType.NESTED_COLLECTOR, order.getValueType());
    assertEquals(OrderType.ASC, order.getOrderType());
    assertEquals("collector", order.getOrderCollectorName());
    assertNotNull(order.getCollectorResultComparator());
  }

  private void assertUnsupported(Runnable r) {
    try {
      r.run();
      fail();
    } catch (UnsupportedOperationException ignored) {
    }
  }
}
