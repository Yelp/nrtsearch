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
package com.yelp.nrtsearch.server.monitoring;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSearcher.LeafSlice;

/** Class to manage the collection of per index metrics. */
public class IndexMetrics {
  public static final Gauge numDocs =
      Gauge.builder()
          .name("nrt_index_num_docs")
          .help("Number of index documents.")
          .labelNames("index")
          .build();
  public static final Gauge numDeletedDocs =
      Gauge.builder()
          .name("nrt_index_num_deleted_docs")
          .help("Number of deleted index documents.")
          .labelNames("index")
          .build();
  public static final Gauge sizeBytes =
      Gauge.builder()
          .name("nrt_index_size_bytes")
          .help("Index size in bytes.")
          .labelNames("index")
          .build();
  public static final Gauge numSegments =
      Gauge.builder()
          .name("nrt_index_num_segments")
          .help("Number of index segments.")
          .labelNames("index")
          .build();
  public static final Gauge segmentDocs =
      Gauge.builder()
          .name("nrt_index_segment_docs")
          .help("Quantiles of documents per segment.")
          .labelNames("index", "quantile")
          .build();
  public static final Gauge numSlices =
      Gauge.builder()
          .name("nrt_index_num_slices")
          .help("Number of index searcher slices.")
          .labelNames("index")
          .build();
  public static final Gauge sliceSegments =
      Gauge.builder()
          .name("nrt_index_slice_segments")
          .help("Quantiles of segments per slice.")
          .labelNames("index", "quantile")
          .build();
  public static final Gauge sliceDocs =
      Gauge.builder()
          .name("nrt_index_slice_docs")
          .help("Quantiles of documents per slice.")
          .labelNames("index", "quantile")
          .build();
  public static final Counter flushCount =
      Counter.builder()
          .name("nrt_index_flush_count")
          .help("Number times the IndexWriter has flushed.")
          .labelNames("index")
          .build();

  public static void updateReaderStats(String index, IndexReader reader) {
    numDocs.labelValues(index).set(reader.numDocs());
    numDeletedDocs.labelValues(index).set(reader.numDeletedDocs());
    numSegments.labelValues(index).set(reader.leaves().size());

    if (!reader.leaves().isEmpty()) {
      ArrayList<LeafReaderContext> sortedLeaves = new ArrayList<>(reader.leaves());
      // sort by segment size
      sortedLeaves.sort(Comparator.comparingInt(l -> l.reader().maxDoc()));
      segmentDocs.labelValues(index, "min").set(sortedLeaves.getFirst().reader().maxDoc());
      segmentDocs.labelValues(index, "0.5").set(getSegmentDocsQuantile(0.5, sortedLeaves));
      segmentDocs.labelValues(index, "0.95").set(getSegmentDocsQuantile(0.95, sortedLeaves));
      segmentDocs.labelValues(index, "0.99").set(getSegmentDocsQuantile(0.99, sortedLeaves));
      segmentDocs.labelValues(index, "max").set(sortedLeaves.getLast().reader().maxDoc());

      try {
        // This type assumption is the same made by the nrt PrimaryNode class
        SegmentInfos infos = ((StandardDirectoryReader) reader).getSegmentInfos();
        long indexSize = 0;
        for (SegmentCommitInfo info : infos) {
          indexSize += info.sizeInBytes();
        }
        sizeBytes.labelValues(index).set(indexSize);
      } catch (Exception ignored) {
        // don't let an exception here stop creation of a new searcher
      }
    }
  }

  public static void updateSearcherStats(String index, IndexSearcher searcher) {
    LeafSlice[] slices = searcher.getSlices();
    numSlices.labelValues(index).set(slices.length);

    if (slices.length > 0) {
      // segments per slice
      int[] segments = new int[slices.length];
      for (int i = 0; i < slices.length; ++i) {
        segments[i] = slices[i].leaves.length;
      }
      Arrays.sort(segments);
      sliceSegments.labelValues(index, "min").set(segments[0]);
      sliceSegments.labelValues(index, "0.5").set(getSliceSegmentsQuantile(0.5, segments));
      sliceSegments.labelValues(index, "0.95").set(getSliceSegmentsQuantile(0.95, segments));
      sliceSegments.labelValues(index, "0.99").set(getSliceSegmentsQuantile(0.99, segments));
      sliceSegments.labelValues(index, "max").set(segments[segments.length - 1]);

      // docs per slice
      long[] docCounts = new long[slices.length];
      for (int i = 0; i < slices.length; ++i) {
        long sliceCount = 0;
        for (int j = 0; j < slices[i].leaves.length; ++j) {
          sliceCount += slices[i].leaves[j].reader().maxDoc();
        }
        docCounts[i] = sliceCount;
      }
      Arrays.sort(docCounts);
      sliceDocs.labelValues(index, "min").set(docCounts[0]);
      sliceDocs.labelValues(index, "0.5").set(getSliceDocsQuantile(0.5, docCounts));
      sliceDocs.labelValues(index, "0.95").set(getSliceDocsQuantile(0.95, docCounts));
      sliceDocs.labelValues(index, "0.99").set(getSliceDocsQuantile(0.99, docCounts));
      sliceDocs.labelValues(index, "max").set(docCounts[docCounts.length - 1]);
    }
  }

  /**
   * Add all index metrics to the collector registry.
   *
   * @param registry collector registry
   */
  public static void register(PrometheusRegistry registry) {
    registry.register(numDocs);
    registry.register(numDeletedDocs);
    registry.register(sizeBytes);
    registry.register(numSegments);
    registry.register(segmentDocs);
    registry.register(numSlices);
    registry.register(sliceSegments);
    registry.register(sliceDocs);
    registry.register(flushCount);
  }

  private static int getSegmentDocsQuantile(double quantile, List<LeafReaderContext> segments) {
    int index = getQuantileIndex(quantile, segments.size());
    return segments.get(index).reader().maxDoc();
  }

  private static int getSliceSegmentsQuantile(double quantile, int[] segments) {
    int index = getQuantileIndex(quantile, segments.length);
    return segments[index];
  }

  private static long getSliceDocsQuantile(double quantile, long[] counts) {
    int index = getQuantileIndex(quantile, counts.length);
    return counts[index];
  }

  private static int getQuantileIndex(double quantile, int size) {
    return ((int) Math.ceil(quantile / 1.0 * size)) - 1;
  }
}
