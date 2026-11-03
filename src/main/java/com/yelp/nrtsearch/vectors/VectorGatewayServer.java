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
package com.yelp.nrtsearch.vectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP server for the S3 Vectors compatible API. Each operation is {@code POST /<OperationName>}
 * with a JSON body, for example {@code POST /QueryVectors}.
 *
 * <p>Started from the command line by {@link VectorGatewayCommand}.
 */
public class VectorGatewayServer {
  private static final Logger logger = LoggerFactory.getLogger(VectorGatewayServer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  public static final int MAX_REQUEST_BYTES = 20 * 1024 * 1024;

  private final VectorService service;
  private final HttpServer server;
  private final ExecutorService executor;
  private final ScheduledExecutorService tieringExecutor;
  private final Map<String, Function<JsonNode, ObjectNode>> operations;

  public VectorGatewayServer(VectorService service, int port) throws IOException {
    this(service, port, 32, 10);
  }

  /**
   * @param service service handling the requests
   * @param port HTTP port, or 0 for any free port
   * @param threads number of request handling threads
   * @param idleCheckSeconds how often to look for idle indexes to stop, when tiering is enabled
   */
  public VectorGatewayServer(VectorService service, int port, int threads, long idleCheckSeconds)
      throws IOException {
    this.service = service;
    this.operations =
        Map.ofEntries(
            Map.entry("CreateVectorBucket", service::createVectorBucket),
            Map.entry("GetVectorBucket", service::getVectorBucket),
            Map.entry("ListVectorBuckets", service::listVectorBuckets),
            Map.entry("DeleteVectorBucket", service::deleteVectorBucket),
            Map.entry("CreateIndex", service::createIndex),
            Map.entry("GetIndex", service::getIndex),
            Map.entry("ListIndexes", service::listIndexes),
            Map.entry("DeleteIndex", service::deleteIndex),
            Map.entry("PutVectors", service::putVectors),
            Map.entry("GetVectors", service::getVectors),
            Map.entry("DeleteVectors", service::deleteVectors),
            Map.entry("ListVectors", service::listVectors),
            Map.entry("QueryVectors", service::queryVectors));
    this.server = HttpServer.create(new InetSocketAddress(port), 0);
    this.executor = Executors.newFixedThreadPool(threads);
    server.setExecutor(executor);
    server.createContext("/", this::handle);

    if (service.getTiering().isEnabled()) {
      tieringExecutor = Executors.newSingleThreadScheduledExecutor();
      tieringExecutor.scheduleWithFixedDelay(
          () -> {
            try {
              service.getTiering().stopIdleIndexes();
            } catch (RuntimeException e) {
              logger.warn("Idle index check failed", e);
            }
          },
          idleCheckSeconds,
          idleCheckSeconds,
          TimeUnit.SECONDS);
    } else {
      tieringExecutor = null;
    }
  }

  public void start() {
    server.start();
    logger.info("Vector gateway listening on port {}", getPort());
  }

  public int getPort() {
    return server.getAddress().getPort();
  }

  public void stop() {
    server.stop(0);
    executor.shutdownNow();
    if (tieringExecutor != null) {
      tieringExecutor.shutdownNow();
    }
  }

  private void handle(HttpExchange exchange) throws IOException {
    try (exchange) {
      String path = exchange.getRequestURI().getPath();
      String operation = path.startsWith("/") ? path.substring(1) : path;
      if (!"POST".equals(exchange.getRequestMethod())) {
        send(exchange, 405, error("ValidationException", "Only POST is supported"));
        return;
      }
      Function<JsonNode, ObjectNode> handler = operations.get(operation);
      if (handler == null) {
        send(exchange, 404, error("UnknownOperationException", "Unknown operation: " + operation));
        return;
      }
      JsonNode request;
      try (InputStream body = exchange.getRequestBody()) {
        byte[] bytes = body.readNBytes(MAX_REQUEST_BYTES + 1);
        if (bytes.length > MAX_REQUEST_BYTES) {
          send(exchange, 413, error("ValidationException", "Request payload is over 20 MiB"));
          return;
        }
        request = bytes.length == 0 ? MAPPER.createObjectNode() : MAPPER.readTree(bytes);
      } catch (IOException e) {
        send(exchange, 400, error("ValidationException", "Request body is not valid JSON"));
        return;
      }
      try {
        send(exchange, 200, handler.apply(request));
      } catch (VectorsException e) {
        send(exchange, e.getHttpStatus(), error(e.getCode(), e.getMessage()));
      } catch (RuntimeException e) {
        logger.error("{} failed", operation, e);
        send(exchange, 500, error("InternalServerException", String.valueOf(e.getMessage())));
      }
    }
  }

  private static ObjectNode error(String code, String message) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("__type", code);
    node.put("message", message);
    return node;
  }

  private static void send(HttpExchange exchange, int status, JsonNode body) throws IOException {
    byte[] bytes = MAPPER.writeValueAsString(body).getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }
}
