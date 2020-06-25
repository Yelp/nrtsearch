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
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

public interface Tar {
  void extractTar(Path sourceFile, Path destDir) throws IOException;

  void extractTar(final TarArchiveInputStream tarArchiveInputStream, final Path destDirectory)
      throws IOException;

  void buildTar(Path sourceDir, Path destinationFile) throws IOException;

  void buildTar(TarArchiveOutputStream tarArchiveOutputStream, Path sourceDir) throws IOException;

  CompressionMode getCompressionMode();

  enum CompressionMode {
    GZIP,
    LZ4
  }
}
