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
package com.yelp.nrtsearch.server.index;

import java.io.IOException;
import java.io.InputStream;

public interface RemoteFileContainer {

  /**
   * Get a RemoteFilePath object for the given path string.
   *
   * @return a RemoteFilePath object representing the given path
   */
  RemoteFilePath getPath();

  /**
   * Check if a file exists in the remote container.
   *
   * @param name the name of the file to check
   * @return true if the file exists, false otherwise
   * @throws IOException if an I/O error occurs
   */
  boolean fileExists(String name) throws IOException;

  /**
   * Open an InputStream to read the specified file from the remote container.
   *
   * @param name the name of the file to read
   * @return an InputStream for reading the file
   * @throws IOException if an I/O error occurs
   */
  InputStream readFile(String name) throws IOException;

  /**
   * Get the length of the specified file in bytes.
   *
   * @param name the name of the file
   * @return the length of the file in bytes
   * @throws IOException if an I/O error occurs
   */
  long fileLength(String name) throws IOException;
}
