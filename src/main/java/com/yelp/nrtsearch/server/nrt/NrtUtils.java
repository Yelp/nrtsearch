/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.nrt;

import static org.apache.lucene.replicator.nrt.Node.VERBOSE_FILES;
import static org.apache.lucene.replicator.nrt.Node.bytesToString;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.Node;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

public class NrtUtils {

  /**
   * Modified version of {@link Node} implementation that opens files with the READONCE io context.
   *
   * @param fileName the name of the file to read
   * @param cache a map to cache file metadata
   * @param node the nrt node reading the metadata
   * @return the metadata of the file, or null if the file is corrupt or does not exist
   * @throws IOException if an I/O error occurs
   */
  public static FileMetaData readOnceLocalFileMetaData(
      String fileName, Map<String, FileMetaData> cache, Node node) throws IOException {

    FileMetaData result;
    if (cache != null) {
      // We may already have this file cached from the last NRT point:
      result = cache.get(fileName);
    } else {
      result = null;
    }

    if (result == null) {
      // Pull from the filesystem
      long checksum;
      long length;
      byte[] header;
      byte[] footer;
      try (IndexInput in = node.getDirectory().openInput(fileName, IOContext.READONCE)) {
        try {
          length = in.length();
          header = CodecUtil.readIndexHeader(in);
          footer = CodecUtil.readFooter(in);
          checksum = CodecUtil.retrieveChecksum(in);
        } catch (@SuppressWarnings("unused") EOFException | CorruptIndexException cie) {
          // File exists but is busted: we must copy it.  This happens when node had crashed,
          // corrupting an un-fsync'd file.  On init we try
          // to delete such unreferenced files, but virus checker can block that, leaving this bad
          // file.
          if (VERBOSE_FILES) {
            node.message("file " + fileName + ": will copy [existing file is corrupt]");
          }
          return null;
        }
        if (VERBOSE_FILES) {
          node.message("file " + fileName + " has length=" + bytesToString(length));
        }
      } catch (@SuppressWarnings("unused") FileNotFoundException | NoSuchFileException e) {
        if (VERBOSE_FILES) {
          node.message("file " + fileName + ": will copy [file does not exist]");
        }
        return null;
      }

      // NOTE: checksum is redundant w/ footer, but we break it out separately because when the bits
      // cross the wire we need direct access to
      // checksum when copying to catch bit flips:
      result = new FileMetaData(header, footer, length, checksum);
    }

    return result;
  }
}
