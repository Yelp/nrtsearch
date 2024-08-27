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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Util class for filesystem operations */
public class FileUtil {

  /**
   * Deletes a directory recursively.
   *
   * @param dir directory to delete
   * @throws IOException in case deletion is unsuccessful
   */
  public static void deleteAllFiles(Path dir) throws IOException {
    if (Files.exists(dir)) {
      if (Files.isRegularFile(dir)) {
        Files.delete(dir);
      } else {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
          for (Path path : stream) {
            if (Files.isDirectory(path)) {
              deleteAllFiles(path);
            } else {
              Files.delete(path);
            }
          }
        }
        Files.delete(dir);
      }
    }
  }

  /**
   * Deletes all files in a directory recursively. If the path does not exist or is not a directory,
   * this method does nothing.
   *
   * @param dir directory to delete files from
   * @throws IOException in case deletion is unsuccessful
   */
  public static void deleteAllFilesInDir(Path dir) throws IOException {
    if (Files.exists(dir) && Files.isDirectory(dir)) {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
        for (Path path : stream) {
          if (Files.isDirectory(path)) {
            deleteAllFiles(path);
          } else {
            Files.delete(path);
          }
        }
      }
    }
  }
}
