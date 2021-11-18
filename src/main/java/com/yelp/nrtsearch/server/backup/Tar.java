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
package com.yelp.nrtsearch.server.backup;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.Collection;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

public interface Tar {
  void extractTar(Path sourceFile, Path destDir) throws IOException;

  void extractTar(final TarArchiveInputStream tarArchiveInputStream, final Path destDirectory)
      throws IOException;

  default void extractTar(
      final InputStream inputStream, final Path destDirectory, final String fileName)
      throws IOException {
    throw new UnsupportedEncodingException(
        "Extracting from fileInputStream not supported by default");
  }

  /**
   * Build a tar at destinationFile with the directory and files at sourceDir. If either
   * filesToInclude or parentDirectoriesToInclude or both are provided only the specified files or
   * files in specified directories or both would be included in the tar.
   */
  void buildTar(
      Path sourceDir,
      Path destinationFile,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude)
      throws IOException;

  /**
   * Build a tar to the destination OutputStream with the directory and files at sourceDir. If
   * either filesToInclude or parentDirectoriesToInclude or both are provided only the specified
   * files or files in specified directories or both would be included in the tar.
   */
  void buildTar(
      Path sourceDir,
      OutputStream destinationStream,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude)
      throws IOException;

  /**
   * Build a tar using the provided tarArchiveOutputStream with the directory and files at
   * sourceDir. If either filesToInclude or parentDirectoriesToInclude or both are provided only the
   * specified files or files in specified directories or both would be included in the tar.
   */
  void buildTar(
      TarArchiveOutputStream tarArchiveOutputStream,
      Path sourceDir,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude)
      throws IOException;

  CompressionMode getCompressionMode();

  enum CompressionMode {
    GZIP,
    LZ4,
    NONE
  }
}
