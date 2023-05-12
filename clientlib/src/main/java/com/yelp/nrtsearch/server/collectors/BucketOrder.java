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

import com.yelp.nrtsearch.server.grpc.BucketOrder.OrderType;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.Collector.CollectorsCase;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import java.util.Comparator;
import java.util.Map;

/** Class that defines how to sort buckets when doing a terms collector aggregation. */
public class BucketOrder {
  public static final BucketOrder DEFAULT_ORDER = new BucketOrder(ValueType.COUNT, OrderType.DESC);
  public static final String COUNT = "_count";
  private final ValueType valueType;
  private final OrderType orderType;

  /** What values will be used to sort buckets. Can be the bucket count or a nested collector. */
  public enum ValueType {
    COUNT,
    NESTED_COLLECTOR
  }

  /**
   * Constructor.
   *
   * @param valueType what values to use for sorting
   * @param orderType if order is ascending or descending
   */
  protected BucketOrder(ValueType valueType, OrderType orderType) {
    this.valueType = valueType;
    this.orderType = orderType;
  }

  /** Get what values to use for sorting. */
  public ValueType getValueType() {
    return valueType;
  }

  /** Get sort direction. */
  public OrderType getOrderType() {
    return orderType;
  }

  /**
   * When using nested collector ordering, provides a comparator to sort results produced by order
   * collector in natural order.
   */
  public Comparator<CollectorResult> getCollectorResultComparator() {
    throw new UnsupportedOperationException();
  }

  /** When using nested collector ordering, provides name of nested collector to sort by. */
  public String getOrderCollectorName() {
    throw new UnsupportedOperationException();
  }

  /** Bucket order implementation for ordering by a nested collector. */
  public static class NestedCollectorOrder extends BucketOrder {
    private final String orderCollectorName;
    private final Comparator<CollectorResult> comparator;

    /**
     * Constructor.
     *
     * @param valueType what values to use for sorting
     * @param orderType if order is ascending or descending
     * @param orderCollectorName ordering collector name
     * @param comparator ordering collector result comparator, natural order
     */
    protected NestedCollectorOrder(
        ValueType valueType,
        OrderType orderType,
        String orderCollectorName,
        Comparator<CollectorResult> comparator) {
      super(valueType, orderType);
      this.orderCollectorName = orderCollectorName;
      this.comparator = comparator;
    }

    @Override
    public Comparator<CollectorResult> getCollectorResultComparator() {
      return comparator;
    }

    @Override
    public String getOrderCollectorName() {
      return orderCollectorName;
    }
  }

  /**
   * Create a {@link BucketOrder} based on the gRPC ordering definition, and the definition of any
   * nested collectors.
   *
   * @param grpcBucketOrder gRPC bucket ordering definition
   * @param nestedCollectors map of any nested collectors
   * @return bucket order
   */
  public static BucketOrder createBucketOrder(
      com.yelp.nrtsearch.server.grpc.BucketOrder grpcBucketOrder,
      Map<String, Collector> nestedCollectors) {
    if (grpcBucketOrder.getKey().equals(COUNT)) {
      return new BucketOrder(ValueType.COUNT, grpcBucketOrder.getOrder());
    }
    Collector nestedCollector = nestedCollectors.get(grpcBucketOrder.getKey());
    if (nestedCollector == null) {
      throw new IllegalArgumentException("Nested collector not found: " + grpcBucketOrder.getKey());
    }
    // currently, max is the only aggregation that can be used for ordering
    if (nestedCollector.getCollectorsCase() == CollectorsCase.MAX) {
      return new NestedCollectorOrder(
          ValueType.NESTED_COLLECTOR,
          grpcBucketOrder.getOrder(),
          grpcBucketOrder.getKey(),
          Comparator.comparingDouble(c -> c.getDoubleResult().getValue()));
    } else {
      throw new IllegalArgumentException(
          "Collector cannot be used for ordering: " + grpcBucketOrder.getKey());
    }
  }
}
