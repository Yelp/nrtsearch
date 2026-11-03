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

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine;

/**
 * Command line entry point for the vector gateway, installed as the {@code vector_gateway} script.
 */
@CommandLine.Command(
    name = "vector_gateway",
    description = "Run an S3 Vectors compatible HTTP API in front of an nrtsearch primary.")
public class VectorGatewayCommand implements Callable<Integer> {

  @CommandLine.Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Show this help message and exit.")
  private boolean help;

  @CommandLine.Option(
      names = {"--port"},
      description = "HTTP port to listen on (default: ${DEFAULT-VALUE})",
      defaultValue = "8080")
  private int port;

  @CommandLine.Option(
      names = {"--nrtsearchHost"},
      description = "Host of the nrtsearch primary (default: ${DEFAULT-VALUE})",
      defaultValue = "localhost")
  private String nrtsearchHost;

  @CommandLine.Option(
      names = {"--nrtsearchPort"},
      description = "gRPC port of the nrtsearch primary (default: ${DEFAULT-VALUE})",
      defaultValue = "50051")
  private int nrtsearchPort;

  @CommandLine.Option(
      names = {"--registryFile"},
      description =
          "JSON file to save buckets, index configs and metadata schemas in. If unset, they are"
              + " kept only in memory and lost when the gateway stops.")
  private Path registryFile;

  @CommandLine.Option(
      names = {"--idleStopSeconds"},
      description =
          "Stop an index after this many seconds without requests, and restore it from S3 on the"
              + " next request. Requires the nrtsearch server to use REMOTE data location."
              + " 0 disables (default: ${DEFAULT-VALUE})",
      defaultValue = "0")
  private long idleStopSeconds;

  @CommandLine.Option(
      names = {"--idleCheckSeconds"},
      description = "How often to look for idle indexes (default: ${DEFAULT-VALUE})",
      defaultValue = "10")
  private long idleCheckSeconds;

  @CommandLine.Option(
      names = {"--quantizedBits"},
      description =
          "Scalar quantization bits for new indexes: 1, 2, 4, 7 or 8, or 0 for full precision"
              + " HNSW (default: ${DEFAULT-VALUE})",
      defaultValue = "7")
  private int quantizedBits;

  @CommandLine.Option(
      names = {"--rescoreOversample"},
      description =
          "Rescore this many times topK hits with full precision vectors when quantized"
              + " (default: ${DEFAULT-VALUE})",
      defaultValue = "3.0")
  private float rescoreOversample;

  @CommandLine.Option(
      names = {"--minNumCandidates"},
      description = "Smallest number of HNSW candidates per query (default: ${DEFAULT-VALUE})",
      defaultValue = "100")
  private int minNumCandidates;

  @CommandLine.Option(
      names = {"--compoundFiles"},
      description =
          "Let new indexes write compound (.cfs) segment files. Off by default, so that replicas"
              + " can lazily load full precision vectors from S3")
  private boolean compoundFiles;

  @CommandLine.Option(
      names = {"--threads"},
      description = "Number of HTTP request threads (default: ${DEFAULT-VALUE})",
      defaultValue = "32")
  private int threads;

  private final CountDownLatch stopLatch = new CountDownLatch(1);
  private volatile VectorGatewayServer gateway;

  public static void main(String[] args) {
    System.exit(new CommandLine(new VectorGatewayCommand()).execute(args));
  }

  @Override
  public Integer call() throws Exception {
    validate();
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(nrtsearchHost, nrtsearchPort)
            .usePlaintext()
            .maxInboundMessageSize(VectorGatewayServer.MAX_REQUEST_BYTES * 4)
            .build();
    try {
      VectorService service =
          new VectorService(
              LuceneServerGrpc.newBlockingStub(channel),
              LuceneServerGrpc.newStub(channel),
              new BucketRegistry(registryFile),
              buildOptions());
      gateway = new VectorGatewayServer(service, port, threads, idleCheckSeconds);
      Thread shutdownHook = new Thread(this::stop);
      Runtime.getRuntime().addShutdownHook(shutdownHook);
      gateway.start();
      System.out.println(
          "Vector gateway listening on port "
              + gateway.getPort()
              + ", nrtsearch at "
              + nrtsearchHost
              + ":"
              + nrtsearchPort);
      stopLatch.await();
      return 0;
    } finally {
      if (gateway != null) {
        gateway.stop();
      }
      channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  /** Stop a running gateway, making {@link #call()} return. */
  public void stop() {
    stopLatch.countDown();
  }

  /** Port the gateway is listening on, or -1 if it has not started. */
  public int getPort() {
    VectorGatewayServer current = gateway;
    return current == null ? -1 : current.getPort();
  }

  @VisibleForTesting
  VectorService.Options buildOptions() {
    VectorService.Options options = new VectorService.Options();
    options.idleStopMs = TimeUnit.SECONDS.toMillis(idleStopSeconds);
    options.quantizedBits = quantizedBits;
    options.rescoreOversample = rescoreOversample;
    options.minNumCandidates = minNumCandidates;
    options.disableCompoundFiles = !compoundFiles;
    return options;
  }

  private void validate() {
    if (port < 0 || port > 65535) {
      throw new CommandLine.ParameterException(
          new CommandLine(this), "--port must be between 0 and 65535");
    }
    if (idleStopSeconds < 0) {
      throw new CommandLine.ParameterException(
          new CommandLine(this), "--idleStopSeconds must be >= 0");
    }
    if (idleCheckSeconds < 1) {
      throw new CommandLine.ParameterException(
          new CommandLine(this), "--idleCheckSeconds must be >= 1");
    }
    if (!java.util.Set.of(0, 1, 2, 4, 7, 8).contains(quantizedBits)) {
      throw new CommandLine.ParameterException(
          new CommandLine(this), "--quantizedBits must be one of 0, 1, 2, 4, 7, 8");
    }
    if (rescoreOversample != 0 && rescoreOversample < 1) {
      throw new CommandLine.ParameterException(
          new CommandLine(this), "--rescoreOversample must be 0 (off) or >= 1");
    }
    if (minNumCandidates < 1 || minNumCandidates > VectorService.MAX_TOP_K) {
      throw new CommandLine.ParameterException(
          new CommandLine(this),
          "--minNumCandidates must be between 1 and " + VectorService.MAX_TOP_K);
    }
    if (threads < 1) {
      throw new CommandLine.ParameterException(new CommandLine(this), "--threads must be >= 1");
    }
  }
}
