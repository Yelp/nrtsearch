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
package com.yelp.nrtsearch.server.luceneserver.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import org.apache.lucene.util.IOUtils;

/** Utility class containing helper methods for interacting with server state. */
public class StateUtils {
  public static final String GLOBAL_STATE_FOLDER = "global_state";
  public static final String GLOBAL_STATE_FILE = "state.json";
  public static final String INDEX_STATE_FILE = "index_state.json";
  public static final ObjectMapper MAPPER = new ObjectMapper();

  private StateUtils() {}

  /**
   * Ensure that the directory for the given path exists, creating it and any parents if needed.
   *
   * @param dirPath path for desired directory
   * @throws IllegalArgumentException if path exists, but is not a directory
   */
  public static void ensureDirectory(Path dirPath) {
    Objects.requireNonNull(dirPath);
    File dirFile = dirPath.toFile();
    if (dirFile.exists() && !dirFile.isDirectory()) {
      throw new IllegalArgumentException("Path: " + dirFile + " is not a directory");
    } else {
      dirFile.mkdirs();
    }
  }

  /**
   * Write the json representation of the given {@link GlobalStateInfo} into a file in the specified
   * directory. Data is written to a temp file, then moved to replace any existing version. File is
   * synced for durability.
   *
   * @param globalStateInfo global state to write
   * @param directory directory to write state file into
   * @param fileName final name of state file
   * @throws IOException on filesystem error
   */
  public static void writeStateToFile(
      GlobalStateInfo globalStateInfo, Path directory, String fileName) throws IOException {
    Objects.requireNonNull(globalStateInfo);
    Objects.requireNonNull(directory);
    Objects.requireNonNull(fileName);

    String stateStr = JsonFormat.printer().print(globalStateInfo);
    writeToFile(stateStr, directory, fileName);
  }

  /**
   * Write the json representation of the given {@link IndexStateInfo} into a file in the specified
   * directory. Data is written to a temp file, then moved to replace any existing version. File is
   * synced for durability.
   *
   * @param stateInfo index state to write
   * @param directory directory to write state file into
   * @param fileName final name of state file
   * @throws IOException on filesystem error
   */
  public static void writeIndexStateToFile(
      IndexStateInfo stateInfo, Path directory, String fileName) throws IOException {
    Objects.requireNonNull(stateInfo);
    Objects.requireNonNull(directory);
    Objects.requireNonNull(fileName);

    String stateStr = JsonFormat.printer().print(stateInfo);
    writeToFile(stateStr, directory, fileName);
  }

  /**
   * Write a string into a file in the specified directory. Data is written to a temp file, then
   * moved to replace any existing version. File is synced for durability.
   *
   * @param stateStr file data string
   * @param directory directory to write state file into
   * @param fileName final name of state file
   * @throws IOException on filesystem error
   */
  public static void writeToFile(String stateStr, Path directory, String fileName)
      throws IOException {
    File tmpStateFile = File.createTempFile(fileName, ".tmp", directory.toFile());
    FileOutputStream fileOutputStream = new FileOutputStream(tmpStateFile);
    try (DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream)) {
      dataOutputStream.writeUTF(stateStr);
    }

    Path tmpStatePath = tmpStateFile.toPath();
    Path destPath = directory.resolve(fileName);
    IOUtils.fsync(tmpStatePath, false);
    Files.move(tmpStatePath, destPath, StandardCopyOption.REPLACE_EXISTING);

    IOUtils.fsync(destPath, false);
    IOUtils.fsync(directory, true);
  }

  /**
   * Read {@link GlobalStateInfo} from json representation in the given file.
   *
   * @param filePath state json file
   * @return global state
   * @throws IOException on filesystem error
   */
  public static GlobalStateInfo readStateFromFile(Path filePath) throws IOException {
    Objects.requireNonNull(filePath);
    String stateStr;
    FileInputStream fileInputStream = new FileInputStream(filePath.toFile());
    try (DataInputStream dataInputStream = new DataInputStream(fileInputStream)) {
      stateStr = dataInputStream.readUTF();
    }
    GlobalStateInfo.Builder stateBuilder = GlobalStateInfo.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(stateStr, stateBuilder);
    return stateBuilder.build();
  }

  /**
   * Read {@link IndexStateInfo} from json representation in the given file.
   *
   * @param filePath state json file
   * @return index state
   * @throws IOException on filesystem error
   */
  public static IndexStateInfo readIndexStateFromFile(Path filePath) throws IOException {
    Objects.requireNonNull(filePath);
    String stateStr;
    FileInputStream fileInputStream = new FileInputStream(filePath.toFile());
    try (DataInputStream dataInputStream = new DataInputStream(fileInputStream)) {
      stateStr = dataInputStream.readUTF();
    }
    IndexStateInfo.Builder stateBuilder = IndexStateInfo.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(stateStr, stateBuilder);
    return stateBuilder.build();
  }
}
