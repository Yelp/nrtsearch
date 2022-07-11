/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import com.yelp.nrtsearch.server.grpc.BucketResult;
import com.yelp.nrtsearch.server.grpc.BucketResult.Bucket;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.luceneserver.field.DoubleFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FloatFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IntFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.LongFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.TextBaseFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.GlobalOrdinalable;
import com.yelp.nrtsearch.server.luceneserver.search.GlobalOrdinalLookup;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreatorContext;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.NestedCollectorManagers.NestedCollectors;
import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.floats.Float2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Supplier;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ScoreMode;

/** Base class for all collector managers that aggregates terms into buckets. */
public abstract class TermsCollectorManager
    implements AdditionalCollectorManager<TermsCollectorManager.TermsCollector, CollectorResult> {

  private final String name;
  private final int size;
  private final NestedCollectorManagers nestedCollectorManagers;

  /**
   * Produce a collector implementation for term aggregation based on the provided {@link
   * com.yelp.nrtsearch.server.grpc.TermsCollector} definition.
   *
   * @param name collection name
   * @param grpcTermsCollector term collection definition from query
   * @param context context info for collector building
   * @param nestedCollectorSuppliers suppliers to create nested collector managers
   */
  public static TermsCollectorManager buildManager(
      String name,
      com.yelp.nrtsearch.server.grpc.TermsCollector grpcTermsCollector,
      CollectorCreatorContext context,
      Map<String, Supplier<AdditionalCollectorManager<? extends Collector, CollectorResult>>>
          nestedCollectorSuppliers) {
    switch (grpcTermsCollector.getTermsSourceCase()) {
      case SCRIPT:
        return new ScriptTermsCollectorManager(
            name, grpcTermsCollector, context, nestedCollectorSuppliers);
      case FIELD:
        FieldDef field = context.getQueryFields().get(grpcTermsCollector.getField());
        if (field == null) {
          throw new IllegalArgumentException("Unknown field: " + grpcTermsCollector.getField());
        }
        if (field instanceof IndexableFieldDef) {
          IndexableFieldDef indexableFieldDef = (IndexableFieldDef) field;
          if (!indexableFieldDef.hasDocValues()) {
            throw new IllegalArgumentException(
                "Terms collection requires doc values for field: " + grpcTermsCollector.getField());
          }
          // Pick implementation based on field type
          if (indexableFieldDef instanceof IntFieldDef) {
            return new IntTermsCollectorManager(
                name, grpcTermsCollector, context, indexableFieldDef, nestedCollectorSuppliers);
          } else if (indexableFieldDef instanceof LongFieldDef) {
            return new LongTermsCollectorManager(
                name, grpcTermsCollector, context, indexableFieldDef, nestedCollectorSuppliers);
          } else if (indexableFieldDef instanceof FloatFieldDef) {
            return new FloatTermsCollectorManager(
                name, grpcTermsCollector, context, indexableFieldDef, nestedCollectorSuppliers);
          } else if (indexableFieldDef instanceof DoubleFieldDef) {
            return new DoubleTermsCollectorManager(
                name, grpcTermsCollector, context, indexableFieldDef, nestedCollectorSuppliers);
          } else if (indexableFieldDef instanceof TextBaseFieldDef) {
            if (indexableFieldDef instanceof GlobalOrdinalable
                && ((GlobalOrdinalable) indexableFieldDef).usesOrdinals()) {
              return new OrdinalTermsCollectorManager(
                  name,
                  grpcTermsCollector,
                  context,
                  indexableFieldDef,
                  (GlobalOrdinalable) indexableFieldDef,
                  nestedCollectorSuppliers);
            } else {
              return new StringTermsCollectorManager(
                  name, grpcTermsCollector, context, indexableFieldDef, nestedCollectorSuppliers);
            }
          }
        } else if (field instanceof VirtualFieldDef) {
          return new VirtualTermsCollectorManager(
              name, grpcTermsCollector, context, (VirtualFieldDef) field, nestedCollectorSuppliers);
        }
        throw new IllegalArgumentException(
            "Terms collection does not support field: "
                + grpcTermsCollector.getField()
                + ", type: "
                + field.getClass().getName());
      default:
        throw new IllegalArgumentException(
            "Unsupported terms source: " + grpcTermsCollector.getTermsSourceCase());
    }
  }

  /**
   * Constructor.
   *
   * @param name collection name
   * @param size max number of buckets
   * @param nestedCollectorSuppliers suppliers to create nested collector managers
   */
  protected TermsCollectorManager(
      String name,
      int size,
      Map<String, Supplier<AdditionalCollectorManager<? extends Collector, CollectorResult>>>
          nestedCollectorSuppliers) {
    this.name = name;
    this.size = size;
    this.nestedCollectorManagers =
        nestedCollectorSuppliers.isEmpty()
            ? null
            : new NestedCollectorManagers(nestedCollectorSuppliers);
  }

  /** Get collection name */
  public String getName() {
    return name;
  }

  @Override
  public void setSearchContext(SearchContext searchContext) {
    if (nestedCollectorManagers != null) {
      nestedCollectorManagers.setSearchContext(searchContext);
    }
  }
  /** Get max number of buckets to return */
  public int getSize() {
    return size;
  }

  /** Get manager for nested aggregation collection. */
  public boolean hasNestedCollectors() {
    return nestedCollectorManagers != null;
  }

  /** Common base class for all {@link TermsCollectorManager} {@link Collector}s. */
  public abstract class TermsCollector implements Collector {
    private final NestedCollectors nestedCollectors;

    protected TermsCollector() {
      if (nestedCollectorManagers != null) {
        nestedCollectors = nestedCollectorManagers.newCollectors();
      } else {
        nestedCollectors = null;
      }
    }

    /**
     * Get the scoring mode of this collector implementation, will be combined with any nested
     * aggregation modes.
     */
    public abstract ScoreMode implementationScoreMode();

    /** Get the collector wrapping any nested aggregations. */
    public NestedCollectors getNestedCollectors() {
      return nestedCollectors;
    }

    @Override
    public ScoreMode scoreMode() {
      ScoreMode scoreMode = implementationScoreMode();
      if (nestedCollectors != null) {
        if (nestedCollectorManagers.scoreMode() != scoreMode) {
          return ScoreMode.COMPLETE;
        }
      }
      return scoreMode;
    }
  }

  /** Bucket entry for Object -> int. */
  private static class ObjectBucketEntry {
    Object key;
    int count;

    ObjectBucketEntry(Object key, int count) {
      this.key = key;
      this.count = count;
    }

    public void update(Object key, int count) {
      this.key = key;
      this.count = count;
    }
  }

  /**
   * Populate a {@link BucketResult.Builder} based on a count map using an Object key.
   *
   * @param bucketBuilder bucket result builder
   * @param counts map containing doc counts for keys
   * @param nestedCollectors collectors for nested aggregations
   */
  void fillBucketResult(
      BucketResult.Builder bucketBuilder,
      Object2IntMap<Object> counts,
      Collection<NestedCollectors> nestedCollectors)
      throws IOException {
    int size = getSize();
    if (counts.size() > 0 && size > 0) {
      // add all map entries into a priority queue, keeping only the top N
      PriorityQueue<ObjectBucketEntry> priorityQueue =
          new PriorityQueue<>(Math.min(counts.size(), size), Comparator.comparingInt(b -> b.count));

      int otherCounts = 0;
      int minimumCount = -1;
      for (Object2IntMap.Entry<Object> entry : counts.object2IntEntrySet()) {
        if (priorityQueue.size() < size) {
          priorityQueue.offer(new ObjectBucketEntry(entry.getKey(), entry.getIntValue()));
          minimumCount = priorityQueue.peek().count;
        } else if (entry.getIntValue() > minimumCount) {
          ObjectBucketEntry reuse = priorityQueue.poll();
          otherCounts += reuse.count;
          reuse.update(entry.getKey(), entry.getIntValue());
          priorityQueue.offer(reuse);
          minimumCount = priorityQueue.peek().count;
        } else {
          otherCounts += entry.getIntValue();
        }
      }

      // the priority queue is a min heap, use a linked list to reverse the order
      LinkedList<Bucket> buckets = new LinkedList<>();
      while (!priorityQueue.isEmpty()) {
        ObjectBucketEntry entry = priorityQueue.poll();
        Bucket.Builder builder =
            Bucket.newBuilder().setKey(entry.key.toString()).setCount(entry.count);
        if (nestedCollectorManagers != null) {
          builder.putAllNestedCollectorResults(
              nestedCollectorManagers.reduce(entry.key, nestedCollectors));
        }
        buckets.addFirst(builder.build());
      }
      bucketBuilder
          .addAllBuckets(buckets)
          .setTotalBuckets(counts.size())
          .setTotalOtherCounts(otherCounts);
    }
  }

  /** Bucket entry for int -> int. */
  private static class IntBucketEntry {
    int key;
    int count;

    IntBucketEntry(int key, int count) {
      this.key = key;
      this.count = count;
    }

    public void update(int key, int count) {
      this.key = key;
      this.count = count;
    }
  }

  /**
   * Populate a {@link BucketResult.Builder} based on a count map using an int key.
   *
   * @param bucketBuilder bucket result builder
   * @param counts map containing doc counts for keys
   * @param nestedCollectors collectors for nested aggregations
   */
  void fillBucketResult(
      BucketResult.Builder bucketBuilder,
      Int2IntMap counts,
      Collection<NestedCollectors> nestedCollectors)
      throws IOException {
    int size = getSize();
    if (counts.size() > 0 && size > 0) {
      // add all map entries into a priority queue, keeping only the top N
      PriorityQueue<IntBucketEntry> priorityQueue =
          new PriorityQueue<>(Math.min(counts.size(), size), Comparator.comparingInt(b -> b.count));

      int otherCounts = 0;
      int minimumCount = -1;
      for (Int2IntMap.Entry entry : counts.int2IntEntrySet()) {
        if (priorityQueue.size() < size) {
          priorityQueue.offer(new IntBucketEntry(entry.getIntKey(), entry.getIntValue()));
          minimumCount = priorityQueue.peek().count;
        } else if (entry.getIntValue() > minimumCount) {
          IntBucketEntry reuse = priorityQueue.poll();
          otherCounts += reuse.count;
          reuse.update(entry.getIntKey(), entry.getIntValue());
          priorityQueue.offer(reuse);
          minimumCount = priorityQueue.peek().count;
        } else {
          otherCounts += entry.getIntValue();
        }
      }

      // the priority queue is a min heap, use a linked list to reverse the order
      LinkedList<Bucket> buckets = new LinkedList<>();
      while (!priorityQueue.isEmpty()) {
        IntBucketEntry entry = priorityQueue.poll();
        Bucket.Builder builder =
            Bucket.newBuilder().setKey(String.valueOf(entry.key)).setCount(entry.count);
        if (nestedCollectorManagers != null) {
          builder.putAllNestedCollectorResults(
              nestedCollectorManagers.reduce(entry.key, nestedCollectors));
        }
        buckets.addFirst(builder.build());
      }
      bucketBuilder
          .addAllBuckets(buckets)
          .setTotalBuckets(counts.size())
          .setTotalOtherCounts(otherCounts);
    }
  }

  /** Bucket entry for long -> int. */
  private static class LongBucketEntry {
    long key;
    int count;

    LongBucketEntry(long key, int count) {
      this.key = key;
      this.count = count;
    }

    public void update(long key, int count) {
      this.key = key;
      this.count = count;
    }
  }

  /**
   * Populate a {@link BucketResult.Builder} based on a count map using a long key.
   *
   * @param bucketBuilder bucket result builder
   * @param counts map containing doc counts for keys
   * @param nestedCollectors collectors for nested aggregations
   */
  void fillBucketResult(
      BucketResult.Builder bucketBuilder,
      Long2IntMap counts,
      Collection<NestedCollectors> nestedCollectors)
      throws IOException {
    int size = getSize();
    if (counts.size() > 0 && size > 0) {
      // add all map entries into a priority queue, keeping only the top N
      PriorityQueue<LongBucketEntry> priorityQueue =
          new PriorityQueue<>(Math.min(counts.size(), size), Comparator.comparingInt(b -> b.count));

      int otherCounts = 0;
      int minimumCount = -1;
      for (Long2IntMap.Entry entry : counts.long2IntEntrySet()) {
        if (priorityQueue.size() < size) {
          priorityQueue.offer(new LongBucketEntry(entry.getLongKey(), entry.getIntValue()));
          minimumCount = priorityQueue.peek().count;
        } else if (entry.getIntValue() > minimumCount) {
          LongBucketEntry reuse = priorityQueue.poll();
          otherCounts += reuse.count;
          reuse.update(entry.getLongKey(), entry.getIntValue());
          priorityQueue.offer(reuse);
          minimumCount = priorityQueue.peek().count;
        } else {
          otherCounts += entry.getIntValue();
        }
      }

      // the priority queue is a min heap, use a linked list to reverse the order
      LinkedList<Bucket> buckets = new LinkedList<>();
      while (!priorityQueue.isEmpty()) {
        LongBucketEntry entry = priorityQueue.poll();
        Bucket.Builder builder =
            Bucket.newBuilder().setKey(String.valueOf(entry.key)).setCount(entry.count);
        if (nestedCollectorManagers != null) {
          builder.putAllNestedCollectorResults(
              nestedCollectorManagers.reduce(entry.key, nestedCollectors));
        }
        buckets.addFirst(builder.build());
      }
      bucketBuilder
          .addAllBuckets(buckets)
          .setTotalBuckets(counts.size())
          .setTotalOtherCounts(otherCounts);
    }
  }

  /** Bucket entry for float -> int. */
  private static class FloatBucketEntry {
    float key;
    int count;

    FloatBucketEntry(float key, int count) {
      this.key = key;
      this.count = count;
    }

    public void update(float key, int count) {
      this.key = key;
      this.count = count;
    }
  }

  /**
   * Populate a {@link BucketResult.Builder} based on a count map using a float key.
   *
   * @param bucketBuilder bucket result builder
   * @param counts map containing doc counts for keys
   * @param nestedCollectors collectors for nested aggregations
   */
  void fillBucketResult(
      BucketResult.Builder bucketBuilder,
      Float2IntMap counts,
      Collection<NestedCollectors> nestedCollectors)
      throws IOException {
    int size = getSize();
    if (counts.size() > 0 && size > 0) {
      // add all map entries into a priority queue, keeping only the top N
      PriorityQueue<FloatBucketEntry> priorityQueue =
          new PriorityQueue<>(Math.min(counts.size(), size), Comparator.comparingInt(b -> b.count));

      int otherCounts = 0;
      int minimumCount = -1;
      for (Float2IntMap.Entry entry : counts.float2IntEntrySet()) {
        if (priorityQueue.size() < size) {
          priorityQueue.offer(new FloatBucketEntry(entry.getFloatKey(), entry.getIntValue()));
          minimumCount = priorityQueue.peek().count;
        } else if (entry.getIntValue() > minimumCount) {
          FloatBucketEntry reuse = priorityQueue.poll();
          otherCounts += reuse.count;
          reuse.update(entry.getFloatKey(), entry.getIntValue());
          priorityQueue.offer(reuse);
          minimumCount = priorityQueue.peek().count;
        } else {
          otherCounts += entry.getIntValue();
        }
      }

      // the priority queue is a min heap, use a linked list to reverse the order
      LinkedList<Bucket> buckets = new LinkedList<>();
      while (!priorityQueue.isEmpty()) {
        FloatBucketEntry entry = priorityQueue.poll();
        Bucket.Builder builder =
            Bucket.newBuilder().setKey(String.valueOf(entry.key)).setCount(entry.count);
        if (nestedCollectorManagers != null) {
          builder.putAllNestedCollectorResults(
              nestedCollectorManagers.reduce(entry.key, nestedCollectors));
        }
        buckets.addFirst(builder.build());
      }
      bucketBuilder
          .addAllBuckets(buckets)
          .setTotalBuckets(counts.size())
          .setTotalOtherCounts(otherCounts);
    }
  }

  /** Bucket entry for double -> int. */
  private static class DoubleBucketEntry {
    double key;
    int count;

    DoubleBucketEntry(double key, int count) {
      this.key = key;
      this.count = count;
    }

    public void update(double key, int count) {
      this.key = key;
      this.count = count;
    }
  }

  /**
   * Populate a {@link BucketResult.Builder} based on a count map using a double key.
   *
   * @param bucketBuilder bucket result builder
   * @param counts map containing doc counts for keys
   * @param nestedCollectors collectors for nested aggregations
   */
  void fillBucketResult(
      BucketResult.Builder bucketBuilder,
      Double2IntMap counts,
      Collection<NestedCollectors> nestedCollectors)
      throws IOException {
    int size = getSize();
    if (counts.size() > 0 && size > 0) {
      // add all map entries into a priority queue, keeping only the top N
      PriorityQueue<DoubleBucketEntry> priorityQueue =
          new PriorityQueue<>(Math.min(counts.size(), size), Comparator.comparingInt(b -> b.count));

      int otherCounts = 0;
      int minimumCount = -1;
      for (Double2IntMap.Entry entry : counts.double2IntEntrySet()) {
        if (priorityQueue.size() < size) {
          priorityQueue.offer(new DoubleBucketEntry(entry.getDoubleKey(), entry.getIntValue()));
          minimumCount = priorityQueue.peek().count;
        } else if (entry.getIntValue() > minimumCount) {
          DoubleBucketEntry reuse = priorityQueue.poll();
          otherCounts += reuse.count;
          reuse.update(entry.getDoubleKey(), entry.getIntValue());
          priorityQueue.offer(reuse);
          minimumCount = priorityQueue.peek().count;
        } else {
          otherCounts += entry.getIntValue();
        }
      }

      // the priority queue is a min heap, use a linked list to reverse the order
      LinkedList<Bucket> buckets = new LinkedList<>();
      while (!priorityQueue.isEmpty()) {
        DoubleBucketEntry entry = priorityQueue.poll();
        Bucket.Builder builder =
            Bucket.newBuilder().setKey(String.valueOf(entry.key)).setCount(entry.count);
        if (nestedCollectorManagers != null) {
          builder.putAllNestedCollectorResults(
              nestedCollectorManagers.reduce(entry.key, nestedCollectors));
        }
        buckets.addFirst(builder.build());
      }
      bucketBuilder
          .addAllBuckets(buckets)
          .setTotalBuckets(counts.size())
          .setTotalOtherCounts(otherCounts);
    }
  }

  /**
   * Populate a {@link BucketResult.Builder} based on a counts of global ordinals.
   *
   * @param bucketBuilder bucket result builder
   * @param counts map containing doc counts for keys
   * @param ordinalLookup global ordinal lookup object
   * @param nestedCollectors collectors for nested aggregations
   */
  void fillBucketResult(
      BucketResult.Builder bucketBuilder,
      Long2IntMap counts,
      GlobalOrdinalLookup ordinalLookup,
      Collection<NestedCollectors> nestedCollectors)
      throws IOException {
    int size = getSize();
    if (counts.size() > 0 && size > 0) {
      // add all map entries into a priority queue, keeping only the top N
      PriorityQueue<LongBucketEntry> priorityQueue =
          new PriorityQueue<>(size, Comparator.comparingInt(b -> b.count));

      int otherCounts = 0;
      int minimumCount = -1;
      for (Long2IntMap.Entry entry : counts.long2IntEntrySet()) {
        if (priorityQueue.size() < size) {
          priorityQueue.offer(new LongBucketEntry(entry.getLongKey(), entry.getIntValue()));
          minimumCount = priorityQueue.peek().count;
        } else if (entry.getIntValue() > minimumCount) {
          LongBucketEntry reuse = priorityQueue.poll();
          otherCounts += reuse.count;
          reuse.update(entry.getLongKey(), entry.getIntValue());
          priorityQueue.offer(reuse);
          minimumCount = priorityQueue.peek().count;
        } else {
          otherCounts += entry.getIntValue();
        }
      }

      // the priority queue is a min heap, use a linked list to reverse the order
      LinkedList<Bucket> buckets = new LinkedList<>();
      while (!priorityQueue.isEmpty()) {
        LongBucketEntry entry = priorityQueue.poll();
        Bucket.Builder builder =
            Bucket.newBuilder()
                .setKey(ordinalLookup.lookupGlobalOrdinal(entry.key))
                .setCount(entry.count);
        if (nestedCollectorManagers != null) {
          builder.putAllNestedCollectorResults(
              nestedCollectorManagers.reduce(entry.key, nestedCollectors));
        }
        buckets.addFirst(builder.build());
      }
      bucketBuilder
          .addAllBuckets(buckets)
          .setTotalBuckets(counts.size())
          .setTotalOtherCounts(otherCounts);
    }
  }
}
