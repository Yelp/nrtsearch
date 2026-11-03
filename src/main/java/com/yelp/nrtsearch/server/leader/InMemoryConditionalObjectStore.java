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
package com.yelp.nrtsearch.server.leader;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In process {@link ConditionalObjectStore}, for tests. Like S3, the version of an object is the
 * MD5 of its content (S3 ETag for single part uploads), so rewriting identical content does not
 * change the version.
 */
public class InMemoryConditionalObjectStore implements ConditionalObjectStore {
  private final ConcurrentHashMap<String, VersionedObject> objects = new ConcurrentHashMap<>();

  @Override
  public Optional<VersionedObject> get(String key) {
    return Optional.ofNullable(objects.get(key));
  }

  @Override
  public String putIfAbsent(String key, byte[] data, Map<String, String> metadata)
      throws IOException {
    VersionedObject obj = newObject(data, metadata);
    if (objects.putIfAbsent(key, obj) != null) {
      throw new PreconditionFailedException("Object already exists: " + key);
    }
    return obj.version();
  }

  @Override
  public String putIfMatch(
      String key, byte[] data, Map<String, String> metadata, String expectedVersion)
      throws IOException {
    VersionedObject obj = newObject(data, metadata);
    boolean[] ok = {false};
    objects.computeIfPresent(
        key,
        (k, current) -> {
          if (current.version().equals(expectedVersion)) {
            ok[0] = true;
            return obj;
          }
          return current;
        });
    if (!ok[0]) {
      throw new PreconditionFailedException("Version mismatch for object: " + key);
    }
    return obj.version();
  }

  /** Unconditionally write an object, for tests. */
  public String put(String key, byte[] data, Map<String, String> metadata) {
    VersionedObject obj = newObject(data, metadata);
    objects.put(key, obj);
    return obj.version();
  }

  /** Version for content, matching the S3 ETag of a single part upload. */
  public static String contentVersion(byte[] data) {
    try {
      return "\"" + HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(data)) + "\"";
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static VersionedObject newObject(byte[] data, Map<String, String> metadata) {
    return new VersionedObject(
        data.clone(), contentVersion(data), metadata == null ? Map.of() : Map.copyOf(metadata));
  }
}
