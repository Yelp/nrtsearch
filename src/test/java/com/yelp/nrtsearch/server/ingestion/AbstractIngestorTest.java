/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.ingestion;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.field.IdFieldDef;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class AbstractIngestorTest {

  private TestIngestor ingestor;
  private GlobalState mockGlobalState;
  private IndexState mockIndexState;

  @Before
  public void setUp() {
    NrtsearchConfig config = mock(NrtsearchConfig.class);
    ingestor = new TestIngestor(config);

    mockGlobalState = mock(GlobalState.class);
    mockIndexState = mock(IndexState.class);
  }

  @Test
  public void testInitialize() throws IOException {
    when(mockGlobalState.getIndexOrThrow("test_index")).thenReturn(mockIndexState);
    ingestor.initialize(mockGlobalState);

    // Should not throw
    ingestor.commit("test_index");
    verify(mockGlobalState, atLeastOnce()).getIndexOrThrow("test_index");
    verify(mockIndexState).commit();
  }

  @Test(expected = IllegalStateException.class)
  public void testCommitWithoutInitialize() throws IOException {
    ingestor.commit("test_index");
  }

  @Test(expected = IllegalStateException.class)
  public void testAddDocumentsWithoutInitialize() throws Exception {
    List<AddDocumentRequest> docs = Collections.emptyList();
    ingestor.addDocuments(docs, "test_index");
  }

  @Test
  public void testStartAndStopBehavior() throws IOException {
    FlagIngestor ingestor = new FlagIngestor(mock(NrtsearchConfig.class));
    assertFalse(ingestor.started);
    assertFalse(ingestor.stopped);

    ingestor.start();
    assertTrue(ingestor.started);

    ingestor.stop();
    assertTrue(ingestor.stopped);
  }

  @Test(expected = IllegalStateException.class)
  public void testDeleteByQueryWithoutInitialize() throws Exception {
    Query query =
        Query.newBuilder()
            .setTermInSetQuery(
                TermInSetQuery.newBuilder()
                    .setField("id")
                    .setLongTerms(TermInSetQuery.LongTerms.newBuilder().addTerms(1L).build())
                    .build())
            .build();
    ingestor.deleteByQuery(List.of(query), "test_index");
  }

  @Test
  public void testDeleteByQueryRequiresValidIndexState() throws Exception {
    Query query =
        Query.newBuilder()
            .setTermInSetQuery(
                TermInSetQuery.newBuilder()
                    .setField("id")
                    .setLongTerms(TermInSetQuery.LongTerms.newBuilder().addTerms(1L).build())
                    .build())
            .build();

    try {
      ingestor.deleteByQuery(List.of(query), "test_index");
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("not initialized"));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testGetIdFieldDefWithoutInitialize() throws IOException {
    ingestor.getIdFieldDef("test_index");
  }

  @Test
  public void testGetIdFieldDefWithNoIdField() throws IOException {
    when(mockGlobalState.getIndexOrThrow("test_index")).thenReturn(mockIndexState);
    when(mockIndexState.getIdFieldDef()).thenReturn(Optional.empty());

    ingestor.initialize(mockGlobalState);

    Optional<IdFieldDef> result = ingestor.getIdFieldDef("test_index");

    assertFalse(result.isPresent());
    verify(mockGlobalState, atLeastOnce()).getIndexOrThrow("test_index");
    verify(mockIndexState).getIdFieldDef();
  }

  @Test
  public void testGetIdFieldDefWithIdField() throws IOException {
    IdFieldDef mockIdFieldDef = mock(IdFieldDef.class);
    when(mockGlobalState.getIndexOrThrow("test_index")).thenReturn(mockIndexState);
    when(mockIndexState.getIdFieldDef()).thenReturn(Optional.of(mockIdFieldDef));

    ingestor.initialize(mockGlobalState);

    Optional<IdFieldDef> result = ingestor.getIdFieldDef("test_index");

    assertTrue(result.isPresent());
    assertEquals(mockIdFieldDef, result.get());
    verify(mockGlobalState, atLeastOnce()).getIndexOrThrow("test_index");
    verify(mockIndexState).getIdFieldDef();
  }

  private static class FlagIngestor extends AbstractIngestor {
    boolean started = false;
    boolean stopped = false;

    public FlagIngestor(NrtsearchConfig config) {
      super(config);
    }

    @Override
    public void start() throws IOException {
      started = true;
    }

    @Override
    public void stop() throws IOException {
      stopped = true;
    }
  }

  // Minimal concrete subclass for testing
  private static class TestIngestor extends AbstractIngestor {
    public TestIngestor(NrtsearchConfig config) {
      super(config);
    }

    @Override
    public void start() throws IOException {
      // no-op
    }

    @Override
    public void stop() throws IOException {
      // no-op
    }
  }
}
