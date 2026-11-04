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
package com.yelp.nrtsearch.tools.vector_bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import com.yelp.nrtsearch.vectors.BucketRegistry;
import com.yelp.nrtsearch.vectors.VectorService;
import com.yelp.nrtsearch.vectors.VectorsException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Sends S3 Vectors API requests, either through the {@link VectorService} code directly to an
 * nrtsearch node over gRPC, or over HTTP to a running vector gateway.
 */
public interface BenchClient extends Closeable {
  ObjectMapper MAPPER = new ObjectMapper();

  /** Send one operation, such as {@code QueryVectors}, and return the response. */
  JsonNode call(String operation, ObjectNode request) throws IOException;

  /** Client that runs the gateway code in process, talking gRPC to one nrtsearch node. */
  class Direct implements BenchClient {
    private final ManagedChannel channel;
    private final VectorService service;

    public Direct(String host, int port, Path registryFile, VectorService.Options options) {
      this.channel =
          ManagedChannelBuilder.forAddress(host, port)
              .usePlaintext()
              .maxInboundMessageSize(256 * 1024 * 1024)
              .build();
      this.service =
          new VectorService(
              LuceneServerGrpc.newBlockingStub(channel),
              LuceneServerGrpc.newStub(channel),
              new BucketRegistry(registryFile),
              options);
    }

    @Override
    public JsonNode call(String operation, ObjectNode request) throws IOException {
      try {
        return switch (operation) {
          case "CreateVectorBucket" -> service.createVectorBucket(request);
          case "GetVectorBucket" -> service.getVectorBucket(request);
          case "DeleteVectorBucket" -> service.deleteVectorBucket(request);
          case "CreateIndex" -> service.createIndex(request);
          case "GetIndex" -> service.getIndex(request);
          case "DeleteIndex" -> service.deleteIndex(request);
          case "PutVectors" -> service.putVectors(request);
          case "GetVectors" -> service.getVectors(request);
          case "DeleteVectors" -> service.deleteVectors(request);
          case "ListVectors" -> service.listVectors(request);
          case "QueryVectors" -> service.queryVectors(request);
          default -> throw new IllegalArgumentException("Unknown operation " + operation);
        };
      } catch (VectorsException e) {
        throw new BenchException(operation, e.getHttpStatus(), e.getCode(), e.getMessage());
      }
    }

    @Override
    public void close() {
      channel.shutdown();
      try {
        channel.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Client for a running vector gateway's HTTP API. */
  class Http implements BenchClient {
    private final HttpClient http;
    private final String baseUrl;

    public Http(String baseUrl, int threads) {
      this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
      this.http =
          HttpClient.newBuilder()
              .connectTimeout(Duration.ofSeconds(10))
              .executor(java.util.concurrent.Executors.newFixedThreadPool(Math.max(1, threads)))
              .build();
    }

    @Override
    public JsonNode call(String operation, ObjectNode request) throws IOException {
      HttpRequest httpRequest =
          HttpRequest.newBuilder(URI.create(baseUrl + "/" + operation))
              .header("Content-Type", "application/json")
              .timeout(Duration.ofMinutes(5))
              .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(request)))
              .build();
      HttpResponse<String> response;
      try {
        response = http.send(httpRequest, HttpResponse.BodyHandlers.ofString());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted calling " + operation, e);
      }
      JsonNode body = MAPPER.readTree(response.body());
      if (response.statusCode() != 200) {
        throw new BenchException(
            operation,
            response.statusCode(),
            body.path("__type").asText("Unknown"),
            body.path("message").asText(response.body()));
      }
      return body;
    }

    @Override
    public void close() {}
  }

  /** An API call that returned an error. */
  class BenchException extends IOException {
    private final int status;
    private final String code;

    public BenchException(String operation, int status, String code, String message) {
      super(operation + " failed with " + status + " " + code + ": " + message);
      this.status = status;
      this.code = code;
    }

    public int getStatus() {
      return status;
    }

    public String getCode() {
      return code;
    }
  }
}
