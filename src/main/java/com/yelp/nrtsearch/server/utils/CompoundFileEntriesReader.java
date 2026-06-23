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
package com.yelp.nrtsearch.server.utils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.NIOFSDirectory;

/**
 * Reads Lucene compound file entries (.cfe) to determine the byte offset and length of each logical
 * sub-file within the corresponding compound data file (.cfs).
 */
public class CompoundFileEntriesReader {
  // Matches Lucene90CompoundFormat constants
  static final String ENTRIES_CODEC = "Lucene90CompoundEntries";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  public record CfeEntry(String name, long offset, long length) {}

  private CompoundFileEntriesReader() {}

  /**
   * Reads the compound file entries from the given .cfe file.
   *
   * @param cfeFile path to the .cfe file
   * @return map from sub-file name (e.g. "_0.vec") to its {@link CfeEntry}
   * @throws IOException on read failure or codec mismatch
   */
  public static Map<String, CfeEntry> readEntries(Path cfeFile) throws IOException {
    Path dir = cfeFile.getParent();
    String fileName = cfeFile.getFileName().toString();
    try (NIOFSDirectory directory = new NIOFSDirectory(dir);
        ChecksumIndexInput in = directory.openChecksumInput(fileName)) {
      // Skip past the index header (magic + codec name + version + 16-byte ID + suffix).
      // We don't have the segment ID here, so we skip using the known header length and
      // validate only the codec name and version via checkHeader (older format read).
      // The header length formula: 4 (magic) + 4 (codec len) + codec bytes + 4 (version)
      //   + 16 (ID) + 1 (suffix len) + suffix bytes.
      // We use skipBytes to consume the ID and suffix after checkHeader validates magic/codec/ver.
      int headerLen = CodecUtil.indexHeaderLength(ENTRIES_CODEC, "");
      int legacyHeaderLen = CodecUtil.headerLength(ENTRIES_CODEC);
      // Skip the 16-byte ID + 1-byte suffix-length + 0 suffix bytes that follow the legacy header
      int skipBytes = headerLen - legacyHeaderLen;
      CodecUtil.checkHeader(in, ENTRIES_CODEC, VERSION_START, VERSION_CURRENT);
      in.skipBytes(skipBytes);

      int numEntries = in.readVInt();
      Map<String, CfeEntry> entries = new HashMap<>(numEntries);
      for (int i = 0; i < numEntries; i++) {
        String name = in.readString();
        long offset = in.readLong();
        long length = in.readLong();
        entries.put(name, new CfeEntry(name, offset, length));
      }
      CodecUtil.checkFooter(in);
      return Collections.unmodifiableMap(entries);
    }
  }
}
