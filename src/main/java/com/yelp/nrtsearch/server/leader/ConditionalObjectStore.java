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
import java.util.Map;
import java.util.Optional;

/**
 * Minimal object store supporting atomic compare-and-set writes of small objects, used for the
 * leader lease and for fencing remote state pointers.
 */
public interface ConditionalObjectStore {

  /**
   * A versioned object.
   *
   * @param data object content
   * @param version opaque version token (e.g. S3 ETag), changes on every write
   * @param metadata user metadata
   */
  record VersionedObject(byte[] data, String version, Map<String, String> metadata) {}

  /** Thrown when a conditional write fails because the object changed. */
  class PreconditionFailedException extends IOException {
    public PreconditionFailedException(String message) {
      super(message);
    }

    public PreconditionFailedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /** Read an object, or empty if it does not exist. */
  Optional<VersionedObject> get(String key) throws IOException;

  /**
   * Write an object only if it does not exist.
   *
   * @return version of the written object
   * @throws PreconditionFailedException if the object already exists
   */
  String putIfAbsent(String key, byte[] data, Map<String, String> metadata) throws IOException;

  /**
   * Write an object only if its current version matches.
   *
   * @return version of the written object
   * @throws PreconditionFailedException if the object version does not match or it does not exist
   */
  String putIfMatch(String key, byte[] data, Map<String, String> metadata, String expectedVersion)
      throws IOException;
}
