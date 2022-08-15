/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.cli;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Class containing utility methods for cli commands. */
public class CliUtils {

  private CliUtils() {}

  /**
   * Merge a parameter string into a message builder. The parameter may be in one of two forms: the
   * json representation of the protobuf message, or an '@' followed by a path to a file containing
   * the json representation of the protobuf message.
   *
   * @param param parameter string
   * @param builder message builder
   * @param <T> builder type
   * @return builder with parameter data merged in
   * @throws IOException on filesystem or protobuf parsing error
   * @throws IllegalArgumentException if param is empty, or path representation is empty
   */
  public static <T extends Message.Builder> T mergeBuilderFromParam(String param, T builder)
      throws IOException {
    if (param.isEmpty()) {
      throw new IllegalArgumentException("Parameter cannot be empty");
    }

    String messageJson;
    if (param.startsWith("@")) {
      // this is a file path
      String pathString = param.substring(1);
      if (pathString.isEmpty()) {
        throw new IllegalArgumentException("Parameter path cannot be empty");
      }
      Path filePath = Path.of(pathString);
      messageJson = Files.readString(filePath);
    } else {
      messageJson = param;
    }
    JsonFormat.parser().merge(messageJson, builder);
    return builder;
  }
}
