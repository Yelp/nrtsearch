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
package com.yelp.nrtsearch.server.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;

/**
 * Merge policy that tries to divide the index into even sized buckets. Prior to any merge
 * decisions, the index segments are divided into n buckets as evenly as possible. Merges are found
 * for each segment set independently as if it were its own index, managed by a {@link
 * TieredMergePolicy}.
 */
public class BucketedTieredMergePolicy extends TieredMergePolicy {
  private final IntSupplier bucketCountSupplier;
  final LinkedList<Set<String>> pendingMerges = new LinkedList<>();

  /** Class to hold a bucket's segment set and current live document count. */
  private static class MergeBucket {
    SegmentInfos bucketInfos;
    long liveDocCount;

    MergeBucket(int majorVersion) {
      bucketInfos = new SegmentInfos(majorVersion);
      liveDocCount = 0;
    }

    void add(SegmentCommitInfo sci, long liveDocs) {
      bucketInfos.add(sci);
      liveDocCount += liveDocs;
    }

    void addAll(Iterable<SegmentCommitInfo> infos, long totalLiveDocs) {
      bucketInfos.addAll(infos);
      liveDocCount += totalLiveDocs;
    }
  }

  /**
   * Interface for a single unit of index segments. This could be an individual segment, or a set of
   * segments that are part of a pending merge.
   */
  private interface MergeUnit {
    /** Get the total number of live documents in the index unit. */
    long getLiveDocCount();

    /** Add the segments this unit represents into a MergeBucket. */
    void addToBucket(MergeBucket bucket);
  }

  /** MergeUnit representing a single index segment. */
  private static class SingleSegment implements MergeUnit {
    final SegmentCommitInfo info;
    final long liveDocCount;

    SingleSegment(SegmentCommitInfo sci) {
      info = sci;
      liveDocCount = (sci.info.maxDoc() - sci.getDelCount());
    }

    @Override
    public long getLiveDocCount() {
      return liveDocCount;
    }

    @Override
    public void addToBucket(MergeBucket bucket) {
      bucket.add(info, liveDocCount);
    }
  }

  /** MergeUnit representing a collection of index segments. */
  private static class MultiSegment implements MergeUnit {
    List<SegmentCommitInfo> infos = new ArrayList<>();
    long liveDocCount = 0;

    void add(SegmentCommitInfo sci) {
      infos.add(sci);
      liveDocCount += (sci.info.maxDoc() - sci.getDelCount());
    }

    @Override
    public long getLiveDocCount() {
      return liveDocCount;
    }

    @Override
    public void addToBucket(MergeBucket bucket) {
      bucket.addAll(infos, liveDocCount);
    }
  }

  /**
   * Interface to run a super class merge function. Like a standard {@link
   * java.util.function.Function}, but throws a checked {@link IOException}.
   */
  @FunctionalInterface
  interface BucketMergeFunc<T, R> {
    R apply(T t) throws IOException;
  }

  /**
   * Constructor.
   *
   * @param bucketCountSupplier provides the current number of buckets for the index, this value may
   *     change between calls
   */
  public BucketedTieredMergePolicy(IntSupplier bucketCountSupplier) {
    this.bucketCountSupplier = bucketCountSupplier;
  }

  @Override
  public MergePolicy.MergeSpecification findMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergePolicy.MergeContext mergeContext)
      throws IOException {

    final int buckets = bucketCountSupplier.getAsInt();
    if (buckets > 1) {
      return findForSegmentInfos(
          buckets, segmentInfos, infos -> super.findMerges(mergeTrigger, infos, mergeContext));
    } else {
      return super.findMerges(mergeTrigger, segmentInfos, mergeContext);
    }
  }

  @Override
  public MergePolicy.MergeSpecification findForcedMerges(
      SegmentInfos segmentInfos,
      int maxSegmentCount,
      Map<SegmentCommitInfo, Boolean> segmentsToMerge,
      MergePolicy.MergeContext mergeContext)
      throws IOException {

    final int buckets = bucketCountSupplier.getAsInt();
    if (buckets > 1) {
      return findForSegmentInfos(
          buckets,
          segmentInfos,
          infos -> super.findForcedMerges(infos, maxSegmentCount, segmentsToMerge, mergeContext));
    } else {
      return super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
    }
  }

  @Override
  public MergePolicy.MergeSpecification findForcedDeletesMerges(
      SegmentInfos segmentInfos, MergePolicy.MergeContext mergeContext) throws IOException {
    final int buckets = bucketCountSupplier.getAsInt();

    if (buckets > 1) {
      return findForSegmentInfos(
          buckets, segmentInfos, infos -> super.findForcedDeletesMerges(infos, mergeContext));
    } else {
      return super.findForcedDeletesMerges(segmentInfos, mergeContext);
    }
  }

  MergePolicy.MergeSpecification findForSegmentInfos(
      int buckets,
      SegmentInfos segmentInfos,
      BucketMergeFunc<SegmentInfos, MergeSpecification> bucketFunc)
      throws IOException {
    // sort index merge units in descending order
    List<MergeUnit> sortedMergeUnits = getMergeUnits(segmentInfos);
    sortedMergeUnits.sort(Comparator.comparingLong(MergeUnit::getLiveDocCount).reversed());

    // create buckets
    PriorityQueue<MergeBucket> bucketQueue =
        new PriorityQueue<>(buckets, Comparator.comparingLong(mb -> mb.liveDocCount));
    for (int i = 0; i < buckets; ++i) {
      bucketQueue.add(new MergeBucket(segmentInfos.getIndexCreatedVersionMajor()));
    }

    // add each merge unit in sequence to the bucket with the lowest live documents
    for (MergeUnit mu : sortedMergeUnits) {
      MergeBucket mb = bucketQueue.poll();
      mu.addToBucket(mb);
      bucketQueue.add(mb);
    }

    // find merges for each bucket and aggregate them
    MergeSpecification aggregateMerges = new MergeSpecification();
    while (!bucketQueue.isEmpty()) {
      MergeBucket mb = bucketQueue.poll();
      if (mb.bucketInfos.size() > 0) {
        MergeSpecification bucketMerges = bucketFunc.apply(mb.bucketInfos);
        if (bucketMerges != null) {
          for (OneMerge om : bucketMerges.merges) {
            aggregateMerges.add(om);
            addPendingMerge(om);
          }
        }
      }
    }
    if (!aggregateMerges.merges.isEmpty()) {
      return aggregateMerges;
    } else {
      return null;
    }
  }

  /**
   * Group the current index segments into merge units.
   *
   * @param infos current index segments info
   * @return unsorted list of merge units
   */
  private List<MergeUnit> getMergeUnits(SegmentInfos infos) {
    // create mapping of segment name to meta data
    Map<String, SegmentCommitInfo> infoMap = new HashMap<>();
    for (SegmentCommitInfo sci : infos) {
      infoMap.put(sci.info.name, sci);
    }

    List<MergeUnit> unitList = new ArrayList<>();

    // group segments that are part of a pending merge
    ListIterator<Set<String>> pendingIterator = pendingMerges.listIterator();
    while (pendingIterator.hasNext()) {
      Set<String> mergeSegments = pendingIterator.next();
      MultiSegment multiSegment = new MultiSegment();
      boolean skip = false;
      for (String name : mergeSegments) {
        SegmentCommitInfo mergeSegmentInfo = infoMap.get(name);
        if (mergeSegmentInfo == null) {
          // this merge is done
          pendingIterator.remove();
          skip = true;
          break;
        }
        multiSegment.add(mergeSegmentInfo);
      }
      if (skip) {
        continue;
      }
      for (String name : mergeSegments) {
        infoMap.remove(name);
      }
      unitList.add(multiSegment);
    }
    // add remaining segments a single merge units
    for (Map.Entry<String, SegmentCommitInfo> entry : infoMap.entrySet()) {
      unitList.add(new SingleSegment(entry.getValue()));
    }
    return unitList;
  }

  /** Add info for this merge to the pendingMerge list. */
  private void addPendingMerge(OneMerge merge) {
    // add to the start of the list, so it will be looked at first when bucketing
    pendingMerges.addFirst(
        merge.segments.stream().map(sci -> sci.info.name).collect(Collectors.toSet()));
  }
}
