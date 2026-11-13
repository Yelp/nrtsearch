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
package com.yelp.nrtsearch.tools.nrt_utils.state;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.leader.EpochPointerFence;
import com.yelp.nrtsearch.server.leader.S3LeaseElector;
import com.yelp.nrtsearch.server.remote.RemoteBackend.IndexResourceType;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import com.yelp.nrtsearch.server.state.BackendGlobalState;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Print the multi-primary leader lease and the fence state of every {@code _current} pointer of a
 * cluster: resource name, writer epoch, and ETag. Each object is printed as one line of JSON, for
 * inspecting and scripting failover tests.
 */
@CommandLine.Command(
    name = GetFenceStateCommand.GET_FENCE_STATE,
    description =
        "Print the leader lease and the epoch of every _current state pointer, one JSON object per"
            + " line")
public class GetFenceStateCommand implements Callable<Integer> {
  public static final String GET_FENCE_STATE = "getFenceState";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @CommandLine.Option(
      names = {"-s", "--serviceName"},
      description = "Name of nrtsearch cluster",
      required = true)
  private String serviceName;

  @CommandLine.Option(
      names = {"-b", "--bucketName"},
      description = "Name of bucket containing state files",
      required = true)
  private String bucketName;

  @CommandLine.Option(
      names = {"--leaseKey"},
      description = "Lease object key, if not the s3_lease default of <serviceName>/leader/lease")
  private String leaseKey;

  @CommandLine.Option(
      names = {"--region"},
      description = "AWS region name, such as us-west-1, us-west-2, us-east-1")
  private String region;

  @CommandLine.Option(
      names = {"-c", "--credsFile"},
      description =
          "File holding AWS credentials; Will use DefaultCredentialProvider if this is unset.")
  private String credsFile;

  @CommandLine.Option(
      names = {"-p", "--credsProfile"},
      description = "Profile to use from creds file; Neglected when credsFile is unset.",
      defaultValue = "default")
  private String credsProfile;

  @CommandLine.Option(
      names = {"--maxRetry"},
      description = "Maximum number of retry attempts for S3 failed requests",
      defaultValue = "20")
  private int maxRetry;

  private S3Util.S3ClientBundle s3ClientBundle;
  private S3Client s3Client;

  @VisibleForTesting
  void setS3ClientBundle(S3Util.S3ClientBundle s3ClientBundle) {
    this.s3ClientBundle = s3ClientBundle;
  }

  @Override
  public Integer call() throws Exception {
    if (s3ClientBundle == null) {
      s3ClientBundle =
          StateCommandUtils.createS3ClientBundle(
              bucketName, region, credsFile, credsProfile, maxRetry);
    }
    s3Client = s3ClientBundle.s3Client();
    String lease = leaseKey != null ? leaseKey : S3LeaseElector.defaultLeaseKey(serviceName);
    printLease(lease);
    printPointer("GLOBAL_STATE", null, S3Backend.getGlobalStateResourcePrefix(serviceName));
    for (String indexIdentifier : listIndexIdentifiers()) {
      for (IndexResourceType type : IndexResourceType.values()) {
        printPointer(
            type.name(),
            indexIdentifier,
            S3Backend.getIndexResourcePrefix(serviceName, indexIdentifier, type));
      }
    }
    return 0;
  }

  /** Unique identifiers (name-id) of the indices in the current global state, by name. */
  private List<String> listIndexIdentifiers() throws IOException {
    S3Backend backend = new S3Backend(bucketName, false, S3Backend.DEFAULT_CONFIG, s3ClientBundle);
    String contents;
    try {
      contents = StateCommandUtils.getGlobalStateFileContents(backend, serviceName);
    } catch (IllegalArgumentException e) {
      // tombstone global state pointer: no indices
      return List.of();
    }
    if (contents == null) {
      return List.of();
    }
    GlobalStateInfo.Builder builder = GlobalStateInfo.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(contents, builder);
    return new TreeMap<>(builder.build().getIndicesMap())
        .entrySet().stream()
            .map(e -> BackendGlobalState.getUniqueIndexName(e.getKey(), e.getValue().getId()))
            .toList();
  }

  private ResponseBytes<GetObjectResponse> get(String key) {
    try {
      return s3Client.getObjectAsBytes(
          GetObjectRequest.builder().bucket(bucketName).key(key).build());
    } catch (NoSuchKeyException e) {
      return null;
    }
  }

  private void printLease(String key) throws Exception {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("type", "LEASE");
    node.put("key", key);
    ResponseBytes<GetObjectResponse> obj = get(key);
    if (obj == null) {
      node.put("exists", false);
    } else {
      node.put("exists", true);
      node.put("etag", obj.response().eTag());
      node.put("lastModified", String.valueOf(obj.response().lastModified()));
      String body = obj.asString(StandardCharsets.UTF_8);
      try {
        JsonNode parsed = MAPPER.readTree(body);
        node.set("lease", parsed);
      } catch (Exception e) {
        node.put("raw", body);
      }
    }
    System.out.println(MAPPER.writeValueAsString(node));
  }

  private void printPointer(String type, String indexIdentifier, String prefix) throws Exception {
    String key = prefix + S3Backend.CURRENT_VERSION;
    ResponseBytes<GetObjectResponse> obj = get(key);
    if (obj == null && indexIdentifier != null) {
      // not every index has every resource (e.g. warming queries)
      return;
    }
    ObjectNode node = MAPPER.createObjectNode();
    node.put("type", type);
    if (indexIdentifier != null) {
      node.put("index", indexIdentifier);
    }
    node.put("key", key);
    if (obj == null) {
      node.put("exists", false);
    } else {
      String body = obj.asString(StandardCharsets.UTF_8);
      node.put("exists", true);
      node.put("value", EpochPointerFence.decodeValue(body));
      node.put("tombstone", EpochPointerFence.isAbsent(body));
      try {
        node.put("epoch", EpochPointerFence.decodeEpoch(body));
      } catch (IllegalStateException e) {
        node.put("malformed", e.getMessage());
      }
      node.put("etag", obj.response().eTag());
      node.put("lastModified", String.valueOf(obj.response().lastModified()));
    }
    System.out.println(MAPPER.writeValueAsString(node));
  }
}
