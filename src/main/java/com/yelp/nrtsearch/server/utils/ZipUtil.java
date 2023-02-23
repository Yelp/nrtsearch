/*
 * Copyright 2023 Yelp Inc.
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to extract Zip files. */
public class ZipUtil {

  private static final Logger logger = LoggerFactory.getLogger(ZipUtil.class);

  /**
   * Extract a zip file.
   *
   * @param inputStream Zip file to extract, provided as an {@link InputStream}
   * @param destination Local directory {@link Path} to extract the zip to
   * @param saveBeforeUnzip If true this will first save the {@code inputStream} to a file and then
   *     extract the file, otherwise this will directly unzip the {@code inputStream}. Direct
   *     unzipping can be faster if the {@code inputStream} is being downloaded from a remote
   *     source, but it may not work for some newer zip compression formats.
   * @return {@link List} of the paths of the extracted files and directories
   */
  public static List<String> extractZip(
      InputStream inputStream, Path destination, boolean saveBeforeUnzip) {
    List<String> extractedFiles = new ArrayList<>();
    if (saveBeforeUnzip) {
      // We need to write the zip to a temporary file since some zip compression formats require
      // a seekable stream (https://bugs.openjdk.org/browse/JDK-8143613,
      // https://commons.apache.org/proper/commons-compress/zip.html)
      Path tmpZipFile = destination.resolve("tmp.zip");
      writeInputStreamToFile(inputStream, tmpZipFile);
      try (ZipFile zipFile = new ZipFile(tmpZipFile.toFile())) {
        zipFile.stream()
            .forEach(
                zipEntry -> {
                  File filePath = destination.resolve(zipEntry.getName()).toFile();
                  extractedFiles.add(zipEntry.getName());
                  if (zipEntry.isDirectory()) {
                    createDirectory(filePath);
                  } else {
                    extractFile(zipFile, zipEntry, filePath);
                  }
                });
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        tmpZipFile.toFile().delete();
      }
      return extractedFiles;
    }

    // Assuming that the zip format does not require a seekable stream and can be directly unzipped
    // without saving the file from the InputStream. If that is not the case then an error will
    // be thrown.
    byte[] buffer = new byte[1024];
    try (ZipInputStream zipInputStream = new ZipInputStream(inputStream)) {
      ZipEntry entry;
      while ((entry = zipInputStream.getNextEntry()) != null) {
        extractedFiles.add(entry.getName());
        File filePath = destination.resolve(entry.getName()).toFile();
        if (entry.isDirectory()) {
          createDirectory(filePath);
        } else {
          extractFile(zipInputStream, filePath, buffer);
        }
        zipInputStream.closeEntry();
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid input, unable to unzip", e);
    }
    return extractedFiles;
  }

  private static void writeInputStreamToFile(InputStream inputStream, Path filePath) {
    try {
      Files.copy(inputStream, filePath);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to write InputStream to file " + filePath, e);
    } finally {
      try {
        inputStream.close();
      } catch (IOException e) {
        logger.error("Unable to close InputStream after writing to " + filePath, e);
      }
    }
  }

  private static void extractFile(ZipInputStream zipInputStream, File filePath, byte[] buffer)
      throws IOException {
    // For some zip archives it is possible that the directory has not been created yet
    if (!filePath.getParentFile().exists()) {
      createDirectory(filePath.getParentFile());
    }
    try (FileOutputStream fos = new FileOutputStream(filePath)) {
      int length;
      while ((length = zipInputStream.read(buffer)) >= 0) {
        fos.write(buffer, 0, length);
      }
    }
  }

  private static void extractFile(ZipFile zipFile, ZipEntry zipEntry, File filePath) {
    // For some zip archives it is possible that the directory has not been created yet
    if (!filePath.getParentFile().exists()) {
      createDirectory(filePath.getParentFile());
    }
    try {
      writeInputStreamToFile(zipFile.getInputStream(zipEntry), filePath.toPath());
    } catch (IOException e) {
      throw new IllegalStateException("Unable to extract zip file to " + filePath, e);
    }
  }

  private static void createDirectory(File dirFile) {
    if (!dirFile.exists() && !dirFile.mkdirs()) {
      throw new IllegalStateException(
          String.format("Unable to create directory %s, cannot extract zip", dirFile));
    }
  }
}
