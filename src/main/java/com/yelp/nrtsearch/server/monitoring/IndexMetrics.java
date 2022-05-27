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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
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
      Gauge.build()
          .name("nrt_index_num_docs")
          .help("Number of index documents.")
          .labelNames("index")
          .create();
  public static final Gauge numDeletedDocs =
      Gauge.build()
          .name("nrt_index_num_deleted_docs")
          .help("Number of deleted index documents.")
          .labelNames("index")
          .create();
  public static final Gauge sizeBytes =
      Gauge.build()
          .name("nrt_index_size_bytes")
          .help("Index size in bytes.")
          .labelNames("index")
          .create();
  public static final Gauge numSegments =
      Gauge.build()
          .name("nrt_index_num_segments")
          .help("Number of index segments.")
          .labelNames("index")
          .create();
  public static final Gauge segmentDocs =
      Gauge.build()
          .name("nrt_index_segment_docs")
          .help("Quantiles of documents per segment.")
          .labelNames("index", "quantile")
          .create();
  public static final Gauge numSlices =
      Gauge.build()
          .name("nrt_index_num_slices")
          .help("Number of index searcher slices.")
          .labelNames("index")
          .create();
  public static final Gauge sliceSegments =
      Gauge.build()
          .name("nrt_index_slice_segments")
          .help("Quantiles of segments per slice.")
          .labelNames("index", "quantile")
          .create();
  public static final Gauge sliceDocs =
      Gauge.build()
          .name("nrt_index_slice_docs")
          .help("Quantiles of documents per slice.")
          .labelNames("index", "quantile")
          .create();
  public static final Counter flushCount =
      Counter.build()
          .name("nrt_index_flush_count")
          .help("Number times the IndexWriter has flushed.")
          .labelNames("index")
          .create();

  public static void updateReaderStats(String index, IndexReader reader) {
    numDocs.labels(index).set(reader.numDocs());
    numDeletedDocs.labels(index).set(reader.numDeletedDocs());
    numSegments.labels(index).set(reader.leaves().size());

    if (reader.leaves().size() > 0) {
      ArrayList<LeafReaderContext> sortedLeaves = new ArrayList<>(reader.leaves());
      // sort by segment size
      sortedLeaves.sort(Comparator.comparingInt(l -> l.reader().maxDoc()));
      segmentDocs.labels(index, "min").set(sortedLeaves.get(0).reader().maxDoc());
      segmentDocs.labels(index, "0.5").set(getSegmentDocsQuantile(0.5, sortedLeaves));
      segmentDocs.labels(index, "0.95").set(getSegmentDocsQuantile(0.95, sortedLeaves));
      segmentDocs.labels(index, "0.99").set(getSegmentDocsQuantile(0.99, sortedLeaves));
      segmentDocs
          .labels(index, "max")
          .set(sortedLeaves.get(sortedLeaves.size() - 1).reader().maxDoc());

      try {
        // This type assumption is the same made by the nrt PrimaryNode class
        SegmentInfos infos = ((StandardDirectoryReader) reader).getSegmentInfos();
        long indexSize = 0;
        for (SegmentCommitInfo info : infos) {
          indexSize += info.sizeInBytes();
        }
        sizeBytes.labels(index).set(indexSize);
      } catch (Exception ignored) {
        // don't let an exception here stop creation of a new searcher
      }
    }
  }

  public static void updateSearcherStats(String index, IndexSearcher searcher) {
    LeafSlice[] slices = searcher.getSlices();
    numSlices.labels(index).set(slices.length);

    if (slices.length > 0) {
      // segments per slice
      int[] segments = new int[slices.length];
      for (int i = 0; i < slices.length; ++i) {
        segments[i] = slices[i].leaves.length;
      }
      Arrays.sort(segments);
      sliceSegments.labels(index, "min").set(segments[0]);
      sliceSegments.labels(index, "0.5").set(getSliceSegmentsQuantile(0.5, segments));
      sliceSegments.labels(index, "0.95").set(getSliceSegmentsQuantile(0.95, segments));
      sliceSegments.labels(index, "0.99").set(getSliceSegmentsQuantile(0.99, segments));
      sliceSegments.labels(index, "max").set(segments[segments.length - 1]);

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
      sliceDocs.labels(index, "min").set(docCounts[0]);
      sliceDocs.labels(index, "0.5").set(getSliceDocsQuantile(0.5, docCounts));
      sliceDocs.labels(index, "0.95").set(getSliceDocsQuantile(0.95, docCounts));
      sliceDocs.labels(index, "0.99").set(getSliceDocsQuantile(0.99, docCounts));
      sliceDocs.labels(index, "max").set(docCounts[docCounts.length - 1]);
    }
  }

  /**
   * Add all index metrics to the collector registry.
   *
   * @param registry collector registry
   */
  public static void register(CollectorRegistry registry) {
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
