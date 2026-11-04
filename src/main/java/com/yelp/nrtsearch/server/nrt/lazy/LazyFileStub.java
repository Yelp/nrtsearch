/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.nrt.lazy;

import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.utils.JsonUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Local placeholder for an index file that stays in the remote backend. The stub is a small JSON
 * file stored next to the index files, under the file's name plus {@link #STUB_SUFFIX}. It holds
 * what is needed to read the file remotely: the remote index identifier and the file metadata
 * (length, header, footer, checksum and the backend name parts).
 */
public class LazyFileStub {
  public static final String STUB_SUFFIX = ".lazy";

  public String indexIdentifier;
  public NrtFileMetaData fileMetaData;

  /** For JSON deserialization. */
  public LazyFileStub() {}

  public LazyFileStub(String indexIdentifier, NrtFileMetaData fileMetaData) {
    this.indexIdentifier = indexIdentifier;
    this.fileMetaData = fileMetaData;
  }

  public static String stubName(String fileName) {
    return fileName + STUB_SUFFIX;
  }

  public static boolean isStubName(String fileName) {
    return fileName.endsWith(STUB_SUFFIX);
  }

  public static String fileNameForStub(String stubName) {
    return stubName.substring(0, stubName.length() - STUB_SUFFIX.length());
  }

  /** Id of the remote file contents, unique per index version file, used as the cache key. */
  public String fileId(String fileName) {
    return indexIdentifier + "/" + S3Backend.getIndexBackendFileName(fileName, fileMetaData);
  }

  /** Stub contents as UTF-8 JSON. */
  public byte[] toBytes() throws IOException {
    return JsonUtils.objectToJsonStr(this).getBytes(StandardCharsets.UTF_8);
  }

  public static LazyFileStub fromBytes(byte[] bytes) throws IOException {
    return JsonUtils.readValue(new String(bytes, StandardCharsets.UTF_8), LazyFileStub.class);
  }

  /**
   * Write the stub for a file directly to the file system, replacing any existing stub. Used on
   * restore, before the index directory is opened.
   */
  public void write(Path dir, String fileName) throws IOException {
    Path stub = dir.resolve(stubName(fileName));
    Path tmp = dir.resolve(stubName(fileName) + ".tmp");
    Files.write(tmp, toBytes());
    Files.move(tmp, stub, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  /** Read the stub for a file from the file system, or null if there is none. */
  public static LazyFileStub read(Path dir, String fileName) throws IOException {
    Path stub = dir.resolve(stubName(fileName));
    if (!Files.exists(stub)) {
      return null;
    }
    return fromBytes(Files.readAllBytes(stub));
  }
}
