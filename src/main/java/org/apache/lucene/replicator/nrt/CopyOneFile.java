/*
 * Copyright 2025 Yelp Inc.
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
package org.apache.lucene.replicator.nrt;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface to replace the Lucene CopyOneFile class, so that we can use separate implementations
 * for gRPC vs remote (S3) replication.
 */
public interface CopyOneFile extends Closeable {
  /**
   * Get the number of bytes copied so far.
   *
   * @return number of bytes copied so far
   */
  long getBytesCopied();

  /**
   * Perform one unit of work copying the file.
   *
   * @return true if the file is fully copied, false otherwise
   * @throws IOException on error
   */
  boolean visit() throws IOException;

  /**
   * Get the file metadata.
   *
   * @return file metadata
   */
  FileMetaData getFileMetaData();

  /**
   * Get the file name.
   *
   * @return file name
   */
  String getFileName();

  /**
   * Get the temporary file name used during copying.
   *
   * @return temporary file name
   */
  String getFileTmpName();

  /**
   * Get the total number of bytes to copy.
   *
   * @return total number of bytes to copy
   */
  long getBytesToCopy();
}
