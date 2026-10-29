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
package com.yelp.nrtsearch.server.nrt.pagecache;

import com.yelp.nrtsearch.server.utils.PageCacheUtils.FileRange;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.StringHelper;

/**
 * Reads the sub-file byte ranges of a compound file from its entries file. This mirrors the entries
 * format read by {@code Lucene90CompoundReader}, which is used by all current Lucene codecs. The
 * entries file holds, for each sub-file, its name without the segment name prefix (e.g. {@code
 * _Lucene104_0.tip}) and its offset and length in the compound data file.
 */
public final class CompoundFileRanges {
  public static final String DATA_EXTENSION = "cfs";
  public static final String ENTRIES_EXTENSION = "cfe";
  static final String ENTRIES_CODEC = "Lucene90CompoundEntries";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = 0;

  private CompoundFileRanges() {}

  /** Get if the file name is a compound data file. */
  public static boolean isCompoundDataFile(String fileName) {
    return DATA_EXTENSION.equals(IndexFileNames.getExtension(fileName));
  }

  /** Get the name of the entries file for a compound data file. */
  public static String entriesFileName(String compoundFileName) {
    return IndexFileNames.stripExtension(compoundFileName) + "." + ENTRIES_EXTENSION;
  }

  /**
   * Read the sub-file ranges for a compound data file. The entries file checksum is verified, but
   * the entries file is not checked against the segment id, since the segment info may not be
   * available yet.
   *
   * @param directory index directory
   * @param compoundFileName compound data file name, ending in {@code .cfs}
   * @return map of sub-file name to its range in the compound data file, in file order
   * @throws IOException if the entries file is missing, has an unknown version, or is corrupt
   */
  public static Map<String, FileRange> read(Directory directory, String compoundFileName)
      throws IOException {
    if (!isCompoundDataFile(compoundFileName)) {
      throw new IllegalArgumentException("Not a compound data file: " + compoundFileName);
    }
    Map<String, FileRange> ranges = new LinkedHashMap<>();
    try (ChecksumIndexInput in = directory.openChecksumInput(entriesFileName(compoundFileName))) {
      Throwable priorE = null;
      try {
        CodecUtil.checkHeader(in, ENTRIES_CODEC, VERSION_START, VERSION_CURRENT);
        in.skipBytes(StringHelper.ID_LENGTH);
        int suffixLength = in.readByte() & 0xFF;
        in.skipBytes(suffixLength);

        int numEntries = in.readVInt();
        for (int i = 0; i < numEntries; ++i) {
          String id = in.readString();
          long offset = in.readLong();
          long length = in.readLong();
          if (ranges.put(id, new FileRange(offset, length)) != null) {
            throw new IOException("Duplicate compound file entry: " + id);
          }
        }
      } catch (Throwable t) {
        priorE = t;
      } finally {
        CodecUtil.checkFooter(in, priorE);
      }
    }
    return Collections.unmodifiableMap(ranges);
  }
}
