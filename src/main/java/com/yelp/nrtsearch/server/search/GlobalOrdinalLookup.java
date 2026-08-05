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
package com.yelp.nrtsearch.server.search;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.LongValues;

/**
 * Class to manage global ordinal lookup operations. Provides lookup to convert segment ordinals to
 * global ordinals. Instances are cached and shared across concurrent requests, so they must be
 * thread-safe. Use {@link #createTermLookup(IndexReader)} to obtain a per-request {@link
 * TermLookup} for converting global ordinals back to term strings.
 */
public abstract class GlobalOrdinalLookup {
  static final LongValues IDENTITY_MAPPING = new IdentityMapping();

  /**
   * Get segment mapping of local to global ordinals.
   *
   * @param segmentIndex index of segment in {@link IndexReader} leaf list
   */
  public abstract LongValues getSegmentMapping(int segmentIndex);

  /** Get the total number of global ordinals. */
  public abstract long getNumOrdinals();

  /**
   * Create a per-request {@link TermLookup} that can resolve global ordinals to term strings. The
   * returned instance is NOT thread-safe and must not be shared across threads or requests.
   *
   * @param reader index reader for the current request
   * @throws IOException on error loading doc values
   */
  public abstract TermLookup createTermLookup(IndexReader reader) throws IOException;

  /**
   * Per-request object for resolving global ordinals to term strings. NOT thread-safe — each
   * request must obtain its own instance via {@link GlobalOrdinalLookup#createTermLookup}.
   */
  public abstract static class TermLookup {

    /**
     * Look up term value for a given global ordinal.
     *
     * @param ord global ordinal
     * @throws IOException on error loading term value
     */
    public abstract String lookupGlobalOrdinal(long ord) throws IOException;
  }

  /** Implementation for field using {@link SortedDocValues}. */
  public static class SortedLookup extends GlobalOrdinalLookup {
    private final OrdinalMap ordinalMap;
    private final long numOrdinals;
    private final String field;

    public SortedLookup(IndexReader reader, String field) throws IOException {
      this.field = field;
      SortedDocValues dv = MultiDocValues.getSortedValues(reader, field);
      if (dv instanceof MultiSortedDocValues) {
        ordinalMap = ((MultiSortedDocValues) dv).mapping;
        numOrdinals = dv.getValueCount();
      } else if (dv != null) {
        // single segment — no OrdinalMap needed, identity mapping
        ordinalMap = null;
        numOrdinals = dv.getValueCount();
      } else {
        ordinalMap = null;
        numOrdinals = 0;
      }
    }

    @Override
    public LongValues getSegmentMapping(int segmentIndex) {
      if (ordinalMap == null) {
        return IDENTITY_MAPPING;
      }
      return ordinalMap.getGlobalOrds(segmentIndex);
    }

    @Override
    public long getNumOrdinals() {
      return numOrdinals;
    }

    @Override
    public TermLookup createTermLookup(IndexReader reader) throws IOException {
      if (numOrdinals == 0) {
        return new EmptyTermLookup(field);
      }
      List<LeafReaderContext> leaves = reader.leaves();
      SortedDocValues[] segmentValues = new SortedDocValues[leaves.size()];
      for (LeafReaderContext leaf : leaves) {
        segmentValues[leaf.ord] = DocValues.getSorted(leaf.reader(), field);
      }
      return new SortedTermLookup(segmentValues, ordinalMap);
    }
  }

  /** Implementation for field using {@link SortedSetDocValues}. */
  public static class SortedSetLookup extends GlobalOrdinalLookup {
    private final OrdinalMap ordinalMap;
    private final long numOrdinals;
    private final String field;

    public SortedSetLookup(IndexReader reader, String field) throws IOException {
      this.field = field;
      SortedSetDocValues dv = MultiDocValues.getSortedSetValues(reader, field);
      if (dv instanceof MultiSortedSetDocValues) {
        ordinalMap = ((MultiSortedSetDocValues) dv).mapping;
        numOrdinals = dv.getValueCount();
      } else if (dv != null) {
        ordinalMap = null;
        numOrdinals = dv.getValueCount();
      } else {
        ordinalMap = null;
        numOrdinals = 0;
      }
    }

    @Override
    public LongValues getSegmentMapping(int segmentIndex) {
      if (ordinalMap == null) {
        return IDENTITY_MAPPING;
      }
      return ordinalMap.getGlobalOrds(segmentIndex);
    }

    @Override
    public long getNumOrdinals() {
      return numOrdinals;
    }

    @Override
    public TermLookup createTermLookup(IndexReader reader) throws IOException {
      if (numOrdinals == 0) {
        return new EmptyTermLookup(field);
      }
      List<LeafReaderContext> leaves = reader.leaves();
      SortedSetDocValues[] segmentValues = new SortedSetDocValues[leaves.size()];
      for (LeafReaderContext leaf : leaves) {
        segmentValues[leaf.ord] = DocValues.getSortedSet(leaf.reader(), field);
      }
      return new SortedSetTermLookup(segmentValues, ordinalMap);
    }
  }

  /** TermLookup for a field with no values. */
  private static class EmptyTermLookup extends TermLookup {
    private final String field;

    EmptyTermLookup(String field) {
      this.field = field;
    }

    @Override
    public String lookupGlobalOrdinal(long ord) {
      throw new IllegalStateException("No ordinals for field: " + field);
    }
  }

  /**
   * TermLookup for SORTED doc values. Routes global ordinal → segment → local ordinal using the
   * cached OrdinalMap, then delegates to per-request segment doc values for the actual term lookup.
   */
  private static class SortedTermLookup extends TermLookup {
    private final SortedDocValues[] segmentValues;
    private final OrdinalMap ordinalMap;

    SortedTermLookup(SortedDocValues[] segmentValues, OrdinalMap ordinalMap) {
      this.segmentValues = segmentValues;
      this.ordinalMap = ordinalMap;
    }

    @Override
    public String lookupGlobalOrdinal(long ord) throws IOException {
      if (ordinalMap == null) {
        // single segment — global ord == local ord
        return segmentValues[0].lookupOrd((int) ord).utf8ToString();
      }
      int segmentIndex = ordinalMap.getFirstSegmentNumber(ord);
      int localOrd = (int) ordinalMap.getFirstSegmentOrd(ord);
      return segmentValues[segmentIndex].lookupOrd(localOrd).utf8ToString();
    }
  }

  /** TermLookup for SORTED_SET doc values. */
  private static class SortedSetTermLookup extends TermLookup {
    private final SortedSetDocValues[] segmentValues;
    private final OrdinalMap ordinalMap;

    SortedSetTermLookup(SortedSetDocValues[] segmentValues, OrdinalMap ordinalMap) {
      this.segmentValues = segmentValues;
      this.ordinalMap = ordinalMap;
    }

    @Override
    public String lookupGlobalOrdinal(long ord) throws IOException {
      if (ordinalMap == null) {
        return segmentValues[0].lookupOrd(ord).utf8ToString();
      }
      int segmentIndex = ordinalMap.getFirstSegmentNumber(ord);
      long localOrd = ordinalMap.getFirstSegmentOrd(ord);
      return segmentValues[segmentIndex].lookupOrd(localOrd).utf8ToString();
    }
  }

  /** Mapping that maps an ordinal to itself. */
  private static class IdentityMapping extends LongValues {

    @Override
    public long get(long index) {
      return index;
    }
  }
}
