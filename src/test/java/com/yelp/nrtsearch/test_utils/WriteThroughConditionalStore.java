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
package com.yelp.nrtsearch.test_utils;

import com.yelp.nrtsearch.server.leader.ConditionalObjectStore;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Test {@link ConditionalObjectStore} over an S3 mock that does not enforce conditional writes.
 * Compare-and-set is done under a JVM wide lock, using a version stored in object metadata, and
 * objects are written through to S3 so that normal (non conditional) readers see them. Only valid
 * when all writers are in the same JVM, which is the case for tests.
 */
public class WriteThroughConditionalStore implements ConditionalObjectStore {
  private static final Object LOCK = new Object();
  private static final String VERSION_KEY = "test-version";

  private final S3Client s3;
  private final String bucket;

  public WriteThroughConditionalStore(S3Client s3, String bucket) {
    this.s3 = s3;
    this.bucket = bucket;
  }

  @Override
  public Optional<VersionedObject> get(String key) throws IOException {
    synchronized (LOCK) {
      return read(key);
    }
  }

  private Optional<VersionedObject> read(String key) {
    try {
      ResponseBytes<GetObjectResponse> bytes = s3.getObjectAsBytes(b -> b.bucket(bucket).key(key));
      Map<String, String> meta = new HashMap<>(bytes.response().metadata());
      String version = meta.remove(VERSION_KEY);
      // objects written outside this store (e.g. by unfenced code) get a content based version
      if (version == null) {
        version = "unversioned-" + java.util.Arrays.hashCode(bytes.asByteArray());
      }
      return Optional.of(new VersionedObject(bytes.asByteArray(), version, meta));
    } catch (NoSuchKeyException e) {
      return Optional.empty();
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public String putIfAbsent(String key, byte[] data, Map<String, String> metadata)
      throws IOException {
    synchronized (LOCK) {
      if (read(key).isPresent()) {
        throw new PreconditionFailedException("Object exists: " + key);
      }
      return write(key, data, metadata);
    }
  }

  @Override
  public String putIfMatch(
      String key, byte[] data, Map<String, String> metadata, String expectedVersion)
      throws IOException {
    synchronized (LOCK) {
      Optional<VersionedObject> current = read(key);
      if (current.isEmpty() || !current.get().version().equals(expectedVersion)) {
        throw new PreconditionFailedException("Version mismatch: " + key);
      }
      return write(key, data, metadata);
    }
  }

  private String write(String key, byte[] data, Map<String, String> metadata) {
    String version = UUID.randomUUID().toString();
    Map<String, String> meta = new HashMap<>(metadata == null ? Map.of() : metadata);
    meta.put(VERSION_KEY, version);
    s3.putObject(b -> b.bucket(bucket).key(key).metadata(meta), RequestBody.fromBytes(data));
    return version;
  }
}
