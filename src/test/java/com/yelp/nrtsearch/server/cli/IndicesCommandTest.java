/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.cli;

import static org.junit.Assert.*;

import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import picocli.CommandLine;

public class IndicesCommandTest extends ServerTestCase {

  private ByteArrayOutputStream testOutput;
  private PrintStream originalSystemOut;

  @Before
  public void setup() {
    testOutput = new ByteArrayOutputStream();
    originalSystemOut = System.out;
    System.setOut(new PrintStream(testOutput));
  }

  @After
  public void cleanUp() {
    System.setOut(originalSystemOut);
  }

  @Override
  protected List<String> getIndices() {
    // Don't create an index by default
    return List.of();
  }

  @Test
  public void testNoIndices() {
    int exitCode = runIndicesCommand();
    assertEquals(0, exitCode);
    String expected =
        String.format("%sNo index found%s", System.lineSeparator(), System.lineSeparator());
    assertEquals(expected, testOutput.toString());
  }

  @Test
  public void testIndices() {
    List<String> indices = new ArrayList<>(List.of("index1", "index2", "index3"));
    indices.forEach(
        index -> {
          CreateIndexRequest request = CreateIndexRequest.newBuilder().setIndexName(index).build();
          getGrpcServer().getBlockingStub().createIndex(request);
        });
    int exitCode = runIndicesCommand();
    assertEquals(0, exitCode);
    String expected =
        String.format(
            "%s%s%s",
            System.lineSeparator(),
            String.join(System.lineSeparator(), indices),
            System.lineSeparator());
    assertEquals(expected, testOutput.toString());
  }

  private int runIndicesCommand() {
    CommandLine cmd = new CommandLine(new LuceneClientCommand());
    return cmd.execute(
        "--hostname=localhost", "--port=" + getGrpcServer().getGlobalState().getPort(), "indices");
  }
}
