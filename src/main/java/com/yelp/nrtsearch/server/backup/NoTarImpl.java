/*
 * Copyright 2021 Yelp Inc.
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

import com.google.inject.Inject;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoTarImpl implements Tar {
  private static final Logger logger = LoggerFactory.getLogger(NoTarImpl.class);

  @Inject
  public NoTarImpl() {}

  @Override
  public void extractTar(Path sourceFile, Path destDir) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void extractTar(InputStream inputStream, Path destDirectory, String fileName)
      throws IOException {
    if (!Files.exists(destDirectory)) {
      Files.createDirectory(destDirectory);
    }
    File file = destDirectory.resolve(fileName).toFile();
    try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
      long copied = IOUtils.copyLarge(inputStream, fileOutputStream);
      logger.debug(
          "Total Size of file in bytes: "
              + file.length()
              + " bytes copied to destination "
              + copied);
    }
  }

  @Override
  public void extractTar(TarArchiveInputStream tarArchiveInputStream, Path destDirectory)
      throws IOException {
    throw new UnsupportedOperationException(
        "NoTarImpl does not support extracting from TarArchiverInputStream");
  }

  @Override
  public void buildTar(
      Path sourceDir,
      Path destinationFile,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude)
      throws IOException {
    throw new UnsupportedOperationException("NoTarImpl only supports streaming mode");
  }

  @Override
  public void buildTar(
      Path sourceDir,
      OutputStream destinationStream,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude)
      throws IOException {
    File file = new File(sourceDir.toString());
    if (file.isFile()) {
      try (FileInputStream fileInputStream = new FileInputStream(file);
          BufferedOutputStream outStream = new BufferedOutputStream(destinationStream)) {
        long copied = IOUtils.copyLarge(fileInputStream, outStream);
        logger.debug("Total Size of file copied to destination is " + copied);
      }
    }
  }

  @Override
  public void buildTar(
      TarArchiveOutputStream tarArchiveOutputStream,
      Path sourceDir,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude)
      throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public CompressionMode getCompressionMode() {
    return CompressionMode.NONE;
  }
}
