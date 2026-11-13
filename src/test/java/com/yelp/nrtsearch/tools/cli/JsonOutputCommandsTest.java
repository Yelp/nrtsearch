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
package com.yelp.nrtsearch.tools.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.server.ServerTestCase;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import picocli.CommandLine;

/** Commands with single line JSON output for scripts: resolveLeader and docCount. */
public class JsonOutputCommandsTest extends ServerTestCase {
  private static final ObjectMapper MAPPER = new ObjectMapper();
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
  protected void initIndex(String name) throws Exception {
    addDocsFromResourceFile(name, "/addDocs.csv");
  }

  private int run(int port, String... args) {
    String[] full = new String[args.length + 2];
    full[0] = "--hostname=localhost";
    full[1] = "--port=" + port;
    System.arraycopy(args, 0, full, 2, args.length);
    return new CommandLine(new NrtsearchClientCommand()).execute(full);
  }

  private JsonNode lastJsonLine() throws Exception {
    String[] lines = testOutput.toString().split(System.lineSeparator());
    for (int i = lines.length - 1; i >= 0; i--) {
      if (lines[i].startsWith("{")) {
        return MAPPER.readTree(lines[i]);
      }
    }
    throw new AssertionError("No JSON output: " + testOutput);
  }

  @Test
  public void testDocCount() throws Exception {
    assertEquals(0, run(getPort(), "docCount", "-i", DEFAULT_TEST_INDEX));
    JsonNode out = lastJsonLine();
    assertEquals(2, out.get("totalHits").asLong());
    assertTrue(out.get("searcherVersion").asLong() > 0);
  }

  @Test
  public void testDocCountUnknownIndex() throws Exception {
    assertEquals(2, run(getPort(), "docCount", "-i", "not_an_index"));
    assertEquals("NOT_FOUND", lastJsonLine().get("error").asText());
  }

  @Test
  public void testResolveLeaderStandalone() throws Exception {
    // a standalone server is not a primary, so it does not know a leader
    assertEquals(2, run(getPort(), "resolveLeader"));
    assertEquals("UNAVAILABLE", lastJsonLine().get("error").asText());
  }

  @Test
  public void testResolveLeaderUnreachable() throws Exception {
    // nothing listens on port 1
    assertEquals(2, run(1, "resolveLeader", "--timeoutMs", "1000"));
    assertEquals("UNAVAILABLE", lastJsonLine().get("error").asText());
  }
}
