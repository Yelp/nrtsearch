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
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * {@link ConditionalObjectStore} backed by AWS S3 conditional writes (If-Match / If-None-Match).
 * Requires a store that enforces these headers; AWS S3 does.
 */
public class S3ConditionalObjectStore implements ConditionalObjectStore {
  private final S3Client s3;
  private final String bucket;

  public S3ConditionalObjectStore(S3Client s3, String bucket) {
    this.s3 = s3;
    this.bucket = bucket;
  }

  @Override
  public Optional<VersionedObject> get(String key) throws IOException {
    try {
      ResponseBytes<GetObjectResponse> bytes = s3.getObjectAsBytes(b -> b.bucket(bucket).key(key));
      GetObjectResponse response = bytes.response();
      return Optional.of(
          new VersionedObject(bytes.asByteArray(), response.eTag(), response.metadata()));
    } catch (NoSuchKeyException e) {
      return Optional.empty();
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        return Optional.empty();
      }
      throw new IOException("Error reading s3://" + bucket + "/" + key, e);
    }
  }

  @Override
  public String putIfAbsent(String key, byte[] data, Map<String, String> metadata)
      throws IOException {
    return put(key, data, metadata, b -> b.ifNoneMatch("*"));
  }

  @Override
  public String putIfMatch(
      String key, byte[] data, Map<String, String> metadata, String expectedVersion)
      throws IOException {
    return put(key, data, metadata, b -> b.ifMatch(expectedVersion));
  }

  private String put(
      String key,
      byte[] data,
      Map<String, String> metadata,
      java.util.function.Consumer<PutObjectRequest.Builder> condition)
      throws IOException {
    PutObjectRequest.Builder builder =
        PutObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .contentLength((long) data.length)
            .metadata(metadata == null ? Map.of() : metadata);
    condition.accept(builder);
    try {
      return s3.putObject(builder.build(), RequestBody.fromBytes(data)).eTag();
    } catch (S3Exception e) {
      // 412 Precondition Failed, 409 ConditionalRequestConflict (concurrent conditional writes),
      // 404 for If-Match on a missing object
      if (e.statusCode() == 412 || e.statusCode() == 409 || e.statusCode() == 404) {
        throw new PreconditionFailedException(
            "Conditional write failed for s3://" + bucket + "/" + key, e);
      }
      throw new IOException("Error writing s3://" + bucket + "/" + key, e);
    }
  }
}
