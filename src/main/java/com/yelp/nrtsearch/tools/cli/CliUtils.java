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
package com.yelp.nrtsearch.tools.cli;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.LuceneServerProto;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Class containing utility methods for cli commands. */
public class CliUtils {

  // A type registry is required to print google.protobuf.Any fields (e.g. collector anyResult).
  // Registering the luceneserver.proto types also registers everything it transitively imports:
  // all nrtsearch protos, plus the Any, Struct, Empty, and wrapper well-known types.
  private static final JsonFormat.Printer JSON_PRINTER =
      JsonFormat.printer()
          .usingTypeRegistry(
              JsonFormat.TypeRegistry.newBuilder()
                  .add(LuceneServerProto.getDescriptor().getMessageTypes())
                  .build());

  private CliUtils() {}

  /**
   * Convert a protobuf message to a JSON string.
   *
   * @param message the protobuf message
   * @return JSON string representation
   * @throws InvalidProtocolBufferException if the message cannot be converted, such as when it
   *     contains a google.protobuf.Any of a type unknown to the cli (e.g. defined by a plugin)
   */
  public static String toJson(Message message) throws InvalidProtocolBufferException {
    return JSON_PRINTER.print(message);
  }

  /**
   * Print a protobuf message to stdout. If asJson is true, prints as JSON; otherwise prints the
   * default protobuf text format.
   *
   * @param message the protobuf message to print
   * @param asJson whether to print as JSON
   * @throws InvalidProtocolBufferException if JSON conversion fails
   */
  public static void printMessage(Message message, boolean asJson)
      throws InvalidProtocolBufferException {
    if (asJson) {
      System.out.println(toJson(message));
    } else {
      System.out.println(message);
    }
  }

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
