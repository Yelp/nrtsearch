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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class VectorGatewayCommandTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static CommandLine commandLine(VectorGatewayCommand command, StringWriter err) {
    CommandLine cmd = new CommandLine(command);
    cmd.setErr(new PrintWriter(err));
    cmd.setOut(new PrintWriter(err));
    return cmd;
  }

  @Test
  public void testDefaults() {
    VectorGatewayCommand command = new VectorGatewayCommand();
    new CommandLine(command).parseArgs();
    VectorService.Options options = command.buildOptions();
    assertEquals(0, options.idleStopMs);
    assertEquals(7, options.quantizedBits);
    assertEquals(3.0f, options.rescoreOversample, 0);
    assertEquals(100, options.minNumCandidates);
  }

  @Test
  public void testOptions() {
    VectorGatewayCommand command = new VectorGatewayCommand();
    new CommandLine(command)
        .parseArgs(
            "--idleStopSeconds=300",
            "--quantizedBits=4",
            "--rescoreOversample=5",
            "--minNumCandidates=200");
    VectorService.Options options = command.buildOptions();
    assertEquals(300_000, options.idleStopMs);
    assertEquals(4, options.quantizedBits);
    assertEquals(5.0f, options.rescoreOversample, 0);
    assertEquals(200, options.minNumCandidates);
  }

  @Test
  public void testHelp() {
    StringWriter out = new StringWriter();
    int exit = commandLine(new VectorGatewayCommand(), out).execute("--help");
    assertEquals(0, exit);
    String help = out.toString();
    assertTrue(help.contains("Usage: vector_gateway"));
    assertTrue(help.contains("--nrtsearchPort"));
    assertTrue(help.contains("default: 50051"));
  }

  @Test
  public void testInvalidOptions() {
    String[][] invalid = {
      {"--quantizedBits=3"},
      {"--idleStopSeconds=-1"},
      {"--idleCheckSeconds=0"},
      {"--rescoreOversample=0.5"},
      {"--minNumCandidates=0"},
      {"--threads=0"},
      {"--port=70000"},
      {"--port=abc"},
      {"--unknown"},
    };
    for (String[] args : invalid) {
      StringWriter err = new StringWriter();
      int exit = commandLine(new VectorGatewayCommand(), err).execute(args);
      assertEquals("expected failure for " + String.join(" ", args), 2, exit);
    }
  }

  @Test
  public void testRunAndStop() throws Exception {
    Path registry = folder.getRoot().toPath().resolve("registry.json");
    VectorGatewayCommand command = new VectorGatewayCommand();
    CommandLine cmd = new CommandLine(command);
    CompletableFuture<Integer> exit =
        CompletableFuture.supplyAsync(
            () -> cmd.execute("--port=0", "--registryFile=" + registry, "--nrtsearchPort=1"));
    long deadline = System.currentTimeMillis() + 10_000;
    while (command.getPort() <= 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
    assertTrue(command.getPort() > 0);

    // bucket APIs don't need nrtsearch
    HttpResponse<String> response =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(
                        URI.create("http://localhost:" + command.getPort() + "/CreateVectorBucket"))
                    .POST(HttpRequest.BodyPublishers.ofString("{\"vectorBucketName\": \"abc\"}"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode());
    assertTrue(Files.readString(registry).contains("\"abc\""));

    command.stop();
    assertEquals(0, (int) exit.get(10, TimeUnit.SECONDS));
  }
}
