/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.grpc.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.clientlib.Node;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver.Listener;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

public class PrimaryFileNameResolverTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String TEST_FILE = "test_nodes.json";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private URI testFileURI() {
    try {
      return new URI(Paths.get(folder.getRoot().toString(), TEST_FILE).toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void writeNodeFile(List<Node> nodes) throws IOException {
    writeFile(OBJECT_MAPPER.writeValueAsString(nodes));
  }

  private void writeFile(String contents) throws IOException {
    String filePathStr = Paths.get(folder.getRoot().toString(), TEST_FILE).toString();
    try (FileOutputStream outputStream = new FileOutputStream(filePathStr)) {
      outputStream.write(contents.getBytes());
    }
  }

  private void startWithNodes(List<Node> nodes, Listener listener) throws IOException {
    startWithNodes(nodes, listener, 0);
  }

  private void startWithNodes(List<Node> nodes, Listener listener, int portOverride)
      throws IOException {
    writeNodeFile(nodes);
    start(listener, portOverride);
  }

  private void start(Listener listener, int portOverride) {
    PrimaryFileNameResolver resolver = null;
    try {
      resolver = new PrimaryFileNameResolver(testFileURI(), OBJECT_MAPPER, 1000, portOverride);
      resolver.start(listener);
    } finally {
      if (resolver != null) {
        resolver.shutdown();
      }
    }
  }

  private void updateNodes(
      List<Node> initial, List<Node> updated, Listener listener, int portOverride)
      throws IOException {
    writeNodeFile(initial);
    PrimaryFileNameResolver resolver = null;
    try {
      resolver = new PrimaryFileNameResolver(testFileURI(), OBJECT_MAPPER, 1000, portOverride);
      resolver.start(listener);
    } finally {
      if (resolver != null) {
        resolver.shutdown();
      } else {
        fail();
      }
    }
    writeNodeFile(updated);
    resolver.new FileChangedTask().run();
  }

  @Test
  public void testStartNoNodes() throws IOException {
    Listener mockListener = mock(Listener.class);
    startWithNodes(Collections.emptyList(), mockListener);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(1)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertEquals(Collections.emptyList(), argumentCaptor.getValue());

    verifyNoMoreInteractions(mockListener);
  }

  @Test
  public void testStartOneNode() throws IOException {
    Listener mockListener = mock(Listener.class);
    startWithNodes(Collections.singletonList(new Node("localhost", 1234)), mockListener);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(1)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertAddressGroup(argumentCaptor.getValue(), 1234);

    verifyNoMoreInteractions(mockListener);
  }

  @Test
  public void testStartMultipleNodes() throws IOException {
    Listener mockListener = mock(Listener.class);
    startWithNodes(
        Arrays.asList(new Node("localhost", 1234), new Node("localhost", 2345)), mockListener);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(1)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertEquals(Collections.emptyList(), argumentCaptor.getValue());

    verifyNoMoreInteractions(mockListener);
  }

  @Test
  public void testStartPortOverride() throws IOException {
    Listener mockListener = mock(Listener.class);
    startWithNodes(Collections.singletonList(new Node("localhost", 1234)), mockListener, 1111);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(1)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertAddressGroup(argumentCaptor.getValue(), 1111);

    verifyNoMoreInteractions(mockListener);
  }

  @Test
  public void testStartNoFile() {
    Listener mockListener = mock(Listener.class);
    start(mockListener, 0);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(1)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertEquals(Collections.emptyList(), argumentCaptor.getValue());

    verifyNoMoreInteractions(mockListener);
  }

  @Test
  public void testStartEmptyFile() throws IOException {
    Listener mockListener = mock(Listener.class);
    writeFile("");
    start(mockListener, 0);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(1)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertEquals(Collections.emptyList(), argumentCaptor.getValue());

    verifyNoMoreInteractions(mockListener);
  }

  @Test
  public void testStartInvalidJson() throws IOException {
    Listener mockListener = mock(Listener.class);
    writeFile("{]");
    start(mockListener, 0);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(1)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertEquals(Collections.emptyList(), argumentCaptor.getValue());

    verifyNoMoreInteractions(mockListener);
  }

  @Test
  public void testStartNull() throws IOException {
    Listener mockListener = mock(Listener.class);
    writeFile("null");
    start(mockListener, 0);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(1)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertEquals(Collections.emptyList(), argumentCaptor.getValue());

    verifyNoMoreInteractions(mockListener);
  }

  @Test
  public void testUpdateEmpty() throws IOException {
    Listener mockListener = mock(Listener.class);
    updateNodes(
        Collections.emptyList(),
        Collections.singletonList(new Node("localhost", 1234)),
        mockListener,
        0);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(2)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertEquals(Collections.emptyList(), argumentCaptor.getAllValues().get(0));
    assertAddressGroup(argumentCaptor.getAllValues().get(1), 1234);

    verifyNoMoreInteractions(mockListener);
  }

  @Test
  public void testUpdateNode() throws IOException {
    Listener mockListener = mock(Listener.class);
    updateNodes(
        Collections.singletonList(new Node("localhost", 1234)),
        Collections.singletonList(new Node("localhost", 2345)),
        mockListener,
        0);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(2)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertAddressGroup(argumentCaptor.getAllValues().get(0), 1234);
    assertAddressGroup(argumentCaptor.getAllValues().get(1), 2345);

    verifyNoMoreInteractions(mockListener);
  }

  @Test
  public void testUpdateNodeSameValue() throws IOException {
    Listener mockListener = mock(Listener.class);
    updateNodes(
        Collections.singletonList(new Node("localhost", 1234)),
        Collections.singletonList(new Node("localhost", 1234)),
        mockListener,
        0);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(1)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertAddressGroup(argumentCaptor.getAllValues().get(0), 1234);

    verifyNoMoreInteractions(mockListener);
  }

  @Test
  public void testUpdateMultipleNodes() throws IOException {
    Listener mockListener = mock(Listener.class);
    updateNodes(
        Collections.singletonList(new Node("localhost", 1234)),
        Arrays.asList(new Node("localhost", 2345), new Node("localhost", 2346)),
        mockListener,
        0);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(2)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertAddressGroup(argumentCaptor.getAllValues().get(0), 1234);
    assertEquals(Collections.emptyList(), argumentCaptor.getAllValues().get(1));

    verifyNoMoreInteractions(mockListener);
  }

  @Test
  public void testUpdatePortOverride() throws IOException {
    Listener mockListener = mock(Listener.class);
    updateNodes(
        Collections.emptyList(),
        Collections.singletonList(new Node("localhost", 1234)),
        mockListener,
        1111);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<EquivalentAddressGroup>> argumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockListener, times(2)).onAddresses(argumentCaptor.capture(), eq(Attributes.EMPTY));
    assertEquals(Collections.emptyList(), argumentCaptor.getAllValues().get(0));
    assertAddressGroup(argumentCaptor.getAllValues().get(1), 1111);

    verifyNoMoreInteractions(mockListener);
  }

  private void assertAddressGroup(List<EquivalentAddressGroup> addressGroups, int expectedPort) {
    assertEquals(1, addressGroups.size());
    EquivalentAddressGroup addressGroup = addressGroups.get(0);
    List<SocketAddress> socketAddresses = addressGroup.getAddresses();
    assertEquals(1, socketAddresses.size());
    SocketAddress socketAddress = socketAddresses.get(0);
    assertEquals("localhost/127.0.0.1:" + expectedPort, socketAddress.toString());
  }
}
