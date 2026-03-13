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
package com.yelp.nrtsearch.server.search.collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.grpc.LastHitInfo;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.index.IndexState;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.search.LazyQueueTopScoreDocCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.junit.Test;

public class RelevanceCollectorTest {

  @Test
  public void testShouldPreloadQueueDefaultTrue() {
    Map<String, String> emptyOptions = Collections.emptyMap();
    assertTrue(
        "shouldPreloadQueue should return true when no options are provided",
        RelevanceCollector.shouldPreloadQueue(emptyOptions));
  }

  @Test
  public void testShouldPreloadQueueExplicitTrue() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "true");
    assertTrue(
        "shouldPreloadQueue should return true when explicitly set to 'true'",
        RelevanceCollector.shouldPreloadQueue(options));
  }

  @Test
  public void testShouldPreloadQueueExplicitTrueUppercase() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "TRUE");
    assertTrue(
        "shouldPreloadQueue should return true when set to 'TRUE' (uppercase)",
        RelevanceCollector.shouldPreloadQueue(options));
  }

  @Test
  public void testShouldPreloadQueueExplicitTrueMixedCase() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "TrUe");
    assertTrue(
        "shouldPreloadQueue should return true when set to 'TrUe' (mixed case)",
        RelevanceCollector.shouldPreloadQueue(options));
  }

  @Test
  public void testShouldPreloadQueueExplicitFalse() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "false");
    assertFalse(
        "shouldPreloadQueue should return false when explicitly set to 'false'",
        RelevanceCollector.shouldPreloadQueue(options));
  }

  @Test
  public void testShouldPreloadQueueExplicitFalseUppercase() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "FALSE");
    assertFalse(
        "shouldPreloadQueue should return false when set to 'FALSE' (uppercase)",
        RelevanceCollector.shouldPreloadQueue(options));
  }

  @Test
  public void testShouldPreloadQueueExplicitFalseMixedCase() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "fAlSe");
    assertFalse(
        "shouldPreloadQueue should return false when set to 'fAlSe' (mixed case)",
        RelevanceCollector.shouldPreloadQueue(options));
  }

  @Test
  public void testShouldPreloadQueueInvalidValue() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "invalid");
    assertFalse(
        "shouldPreloadQueue should return false when set to an invalid value",
        RelevanceCollector.shouldPreloadQueue(options));
  }

  @Test
  public void testShouldPreloadQueueEmptyValue() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "");
    assertFalse(
        "shouldPreloadQueue should return false when set to empty string",
        RelevanceCollector.shouldPreloadQueue(options));
  }

  @Test
  public void testShouldPreloadQueueWithOtherOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("SOME_OTHER_OPTION", "value");
    options.put("PRELOAD_COLLECTOR_QUEUE", "false");
    options.put("ANOTHER_OPTION", "another_value");
    assertFalse(
        "shouldPreloadQueue should work correctly when mixed with other options",
        RelevanceCollector.shouldPreloadQueue(options));
  }

  @Test
  public void testShouldPreloadQueueNumericValues() {
    Map<String, String> options = new HashMap<>();

    // Test numeric values that should be treated as false
    options.put("PRELOAD_COLLECTOR_QUEUE", "0");
    assertFalse(
        "shouldPreloadQueue should return false for numeric value '0'",
        RelevanceCollector.shouldPreloadQueue(options));

    options.put("PRELOAD_COLLECTOR_QUEUE", "1");
    assertFalse(
        "shouldPreloadQueue should return false for numeric value '1'",
        RelevanceCollector.shouldPreloadQueue(options));
  }

  @Test
  public void testShouldPreloadQueueWhitespaceValues() {
    Map<String, String> options = new HashMap<>();

    // Test whitespace-only values
    options.put("PRELOAD_COLLECTOR_QUEUE", "   ");
    assertFalse(
        "shouldPreloadQueue should return false for whitespace-only value",
        RelevanceCollector.shouldPreloadQueue(options));

    // Test values with leading/trailing whitespace
    options.put("PRELOAD_COLLECTOR_QUEUE", " true ");
    assertTrue(
        "shouldPreloadQueue should return true for 'true' with whitespace (trimming)",
        RelevanceCollector.shouldPreloadQueue(options));

    options.put("PRELOAD_COLLECTOR_QUEUE", " false ");
    assertFalse(
        "shouldPreloadQueue should return false for 'false' with whitespace",
        RelevanceCollector.shouldPreloadQueue(options));
  }

  @Test
  public void testRelevanceCollectorCreatesTopScoreDocCollectorManagerWhenPreloadTrue() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "true");

    SearchRequest request =
        SearchRequest.newBuilder().setTopHits(10).putAllAdditionalOptions(options).build();

    IndexState mockIndexState = createMockIndexState();
    CollectorCreatorContext context =
        new CollectorCreatorContext(request, mockIndexState, null, Collections.emptyMap(), null);

    RelevanceCollector collector = new RelevanceCollector(context, Collections.emptyList());

    assertTrue(
        "RelevanceCollector should create TopScoreDocCollectorManager when preload is true",
        collector.getManager() instanceof TopScoreDocCollectorManager);
  }

  @Test
  public void testRelevanceCollectorCreatesLazyQueueTopScoreDocCollectorManagerWhenPreloadFalse() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "false");

    SearchRequest request =
        SearchRequest.newBuilder().setTopHits(10).putAllAdditionalOptions(options).build();

    IndexState mockIndexState = createMockIndexState();
    CollectorCreatorContext context =
        new CollectorCreatorContext(request, mockIndexState, null, Collections.emptyMap(), null);

    RelevanceCollector collector = new RelevanceCollector(context, Collections.emptyList());

    assertTrue(
        "RelevanceCollector should create LazyQueueTopScoreDocCollectorManager when preload is false",
        collector.getManager() instanceof LazyQueueTopScoreDocCollectorManager);
  }

  @Test
  public void testRelevanceCollectorCreatesTopScoreDocCollectorManagerWhenPreloadDefault() {
    // No additional options provided, should default to true
    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).build();

    IndexState mockIndexState = createMockIndexState();
    CollectorCreatorContext context =
        new CollectorCreatorContext(request, mockIndexState, null, Collections.emptyMap(), null);

    RelevanceCollector collector = new RelevanceCollector(context, Collections.emptyList());

    assertTrue(
        "RelevanceCollector should create TopScoreDocCollectorManager by default",
        collector.getManager() instanceof TopScoreDocCollectorManager);
  }

  @Test
  public void testRelevanceCollectorCreatesLazyQueueTopScoreDocCollectorManagerWithInvalidValue() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "invalid");

    SearchRequest request =
        SearchRequest.newBuilder().setTopHits(10).putAllAdditionalOptions(options).build();

    IndexState mockIndexState = createMockIndexState();
    CollectorCreatorContext context =
        new CollectorCreatorContext(request, mockIndexState, null, Collections.emptyMap(), null);

    RelevanceCollector collector = new RelevanceCollector(context, Collections.emptyList());

    assertTrue(
        "RelevanceCollector should create LazyQueueTopScoreDocCollectorManager with invalid value",
        collector.getManager() instanceof LazyQueueTopScoreDocCollectorManager);
  }

  @Test
  public void testRelevanceCollectorCreatesTopScoreDocCollectorManagerCaseInsensitive() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "TRUE");

    SearchRequest request =
        SearchRequest.newBuilder().setTopHits(10).putAllAdditionalOptions(options).build();

    IndexState mockIndexState = createMockIndexState();
    CollectorCreatorContext context =
        new CollectorCreatorContext(request, mockIndexState, null, Collections.emptyMap(), null);

    RelevanceCollector collector = new RelevanceCollector(context, Collections.emptyList());

    assertTrue(
        "RelevanceCollector should create TopScoreDocCollectorManager with 'TRUE' (case insensitive)",
        collector.getManager() instanceof TopScoreDocCollectorManager);
  }

  @Test
  public void testRelevanceCollectorCreatesLazyQueueTopScoreDocCollectorManagerCaseInsensitive() {
    Map<String, String> options = new HashMap<>();
    options.put("PRELOAD_COLLECTOR_QUEUE", "FALSE");

    SearchRequest request =
        SearchRequest.newBuilder().setTopHits(10).putAllAdditionalOptions(options).build();

    IndexState mockIndexState = createMockIndexState();
    CollectorCreatorContext context =
        new CollectorCreatorContext(request, mockIndexState, null, Collections.emptyMap(), null);

    RelevanceCollector collector = new RelevanceCollector(context, Collections.emptyList());

    assertTrue(
        "RelevanceCollector should create LazyQueueTopScoreDocCollectorManager with 'FALSE' (case insensitive)",
        collector.getManager() instanceof LazyQueueTopScoreDocCollectorManager);
  }

  @Test
  public void testRelevanceCollectorWithOtherOptionsPresent() {
    Map<String, String> options = new HashMap<>();
    options.put("SOME_OTHER_OPTION", "value1");
    options.put("PRELOAD_COLLECTOR_QUEUE", "false");
    options.put("ANOTHER_OPTION", "value2");

    SearchRequest request =
        SearchRequest.newBuilder().setTopHits(10).putAllAdditionalOptions(options).build();

    IndexState mockIndexState = createMockIndexState();
    CollectorCreatorContext context =
        new CollectorCreatorContext(request, mockIndexState, null, Collections.emptyMap(), null);

    RelevanceCollector collector = new RelevanceCollector(context, Collections.emptyList());

    assertTrue(
        "RelevanceCollector should create LazyQueueTopScoreDocCollectorManager when mixed with other options",
        collector.getManager() instanceof LazyQueueTopScoreDocCollectorManager);
  }

  @Test
  public void testRelevanceCollectorWithSearchAfter() {
    LastHitInfo searchAfter = LastHitInfo.newBuilder().setLastDocId(5).setLastScore(1.5f).build();
    SearchRequest request =
        SearchRequest.newBuilder().setTopHits(10).setSearchAfter(searchAfter).build();

    IndexState mockIndexState = createMockIndexState();
    CollectorCreatorContext context =
        new CollectorCreatorContext(request, mockIndexState, null, Collections.emptyMap(), null);

    RelevanceCollector collector = new RelevanceCollector(context, Collections.emptyList());

    assertNotNull(
        "RelevanceCollector with searchAfter should create a non-null manager",
        collector.getManager());
    assertTrue(
        "RelevanceCollector with searchAfter should create TopScoreDocCollectorManager by default",
        collector.getManager() instanceof TopScoreDocCollectorManager);
  }

  @Test
  public void testRelevanceCollectorWithoutSearchAfter() {
    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).build();

    IndexState mockIndexState = createMockIndexState();
    CollectorCreatorContext context =
        new CollectorCreatorContext(request, mockIndexState, null, Collections.emptyMap(), null);

    RelevanceCollector collector = new RelevanceCollector(context, Collections.emptyList());

    assertNotNull(
        "RelevanceCollector without searchAfter should create a non-null manager",
        collector.getManager());
  }

  /**
   * Creates a mock IndexState with the minimum required behavior for RelevanceCollector
   * construction.
   */
  private static IndexState createMockIndexState() {
    IndexState mockIndexState = mock(IndexState.class);
    when(mockIndexState.getDefaultSearchTimeoutSec()).thenReturn(0.0);
    when(mockIndexState.getDefaultSearchTimeoutCheckEvery()).thenReturn(0);
    return mockIndexState;
  }
}
