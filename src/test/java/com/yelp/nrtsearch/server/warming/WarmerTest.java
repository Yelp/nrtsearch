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
package com.yelp.nrtsearch.server.warming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.FunctionFilterQuery;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.KnnRetriever;
import com.yelp.nrtsearch.server.grpc.MultiRetrieverRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Rescorer;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.grpc.TextRetriever;
import com.yelp.nrtsearch.server.handler.SearchHandler;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.monitoring.WarmingMetrics;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot.HistogramDataPointSnapshot;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.s3.S3Client;

public class WarmerTest {

  private final String service = "test_service";
  private final String index = "test_index";
  private final String bucketName = "warmer-unittest";
  private RemoteBackend remoteBackend;
  private S3Client s3;
  private Warmer warmer;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  @Rule public final AmazonS3Provider s3Provider = new AmazonS3Provider(bucketName);

  @Before
  public void setup() throws IOException {
    String configStr = "bucketName: " + bucketName;
    NrtsearchConfig config = new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));
    s3 = s3Provider.getAmazonS3();
    remoteBackend =
        new S3Backend(config, new S3Util.S3ClientBundle(s3, s3Provider.getS3AsyncClient()));
    warmer = new Warmer(remoteBackend, service, index, 4);
  }

  @Test
  public void testAddSearchRequest_backupWarmingQueriesToS3() throws IOException {
    List<SearchRequest> testRequests = getTestSearchRequests();

    testRequests.forEach(r -> warmer.maybeAddSearchRequest(r, 0, null));

    warmer.backupWarmingQueriesToS3(service);

    InputStream queriesStream = remoteBackend.downloadWarmingQueries(service, index);
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(queriesStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    }

    Assertions.assertThat(lines).containsAll(getTestSearchRequestsAsJsonStrings());
  }

  @Test
  public void testWarmFromS3()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    List<String> testSearchRequestsJson = getTestSearchRequestsAsJsonStrings();
    byte[] warmingBytes = getWarmingBytes(testSearchRequestsJson);
    remoteBackend.uploadWarmingQueries(service, "test_index", warmingBytes);

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);

    warmer.warmFromS3(mockIndexState, 0, mockSearchHandler);

    for (SearchRequest testRequest : getTestSearchRequests()) {
      verify(mockSearchHandler).handle(mockIndexState, testRequest);
    }
    verifyNoMoreInteractions(mockSearchHandler);
  }

  @Test
  public void testWarmFromS3_basic()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    Warmer warmerWithBasic = new Warmer(remoteBackend, service, index, 4, 80);
    // nextInt(100) with seed 98795 will always return 15, 76, 87, 97; If we set the Perc at 80,
    // first two queries will be stripped while the last two will be kept
    warmerWithBasic.randomThreadLocal.get().setSeed(98795);

    List<String> testSearchRequestsJson = getTestSearchRequestsAsJsonStrings();
    byte[] warmingBytes = getWarmingBytes(testSearchRequestsJson);
    remoteBackend.uploadWarmingQueries(service, "test_index", warmingBytes);

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);

    warmerWithBasic.warmFromS3(mockIndexState, 0, mockSearchHandler);

    for (SearchRequest testRequest : getTestBasicSearchRequests()) {
      verify(mockSearchHandler).handle(mockIndexState, testRequest);
    }
    verifyNoMoreInteractions(mockSearchHandler);
  }

  @Test
  public void testWarmFromS3_multiple()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    List<String> testSearchRequestsJson = getTestSearchRequestsAsJsonStrings();
    byte[] warmingBytes = getWarmingBytes(testSearchRequestsJson);
    remoteBackend.uploadWarmingQueries(service, "test_index", warmingBytes);

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);

    warmer.warmFromS3(mockIndexState, 0, mockSearchHandler);
    warmer.warmFromS3(mockIndexState, 0, mockSearchHandler);

    for (SearchRequest testRequest : getTestSearchRequests()) {
      verify(mockSearchHandler, times(2)).handle(mockIndexState, testRequest);
    }
    verifyNoMoreInteractions(mockSearchHandler);
  }

  @Test
  public void testWarmFromS3_parallel()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    int warmingCountPerQuery = 10;
    List<String> testSearchRequestsJson = getTestSearchRequestsAsJsonStrings();
    List<String> moreTestSearchRequestsJson = new ArrayList<>();
    for (int i = 0; i < warmingCountPerQuery; i++) {
      moreTestSearchRequestsJson.addAll(testSearchRequestsJson);
    }
    byte[] warmingBytes = getWarmingBytes(moreTestSearchRequestsJson);
    remoteBackend.uploadWarmingQueries(service, "test_index", warmingBytes);

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);

    warmer.warmFromS3(mockIndexState, 3, mockSearchHandler);

    for (SearchRequest testRequest : getTestSearchRequests()) {
      verify(mockSearchHandler, times(warmingCountPerQuery)).handle(mockIndexState, testRequest);
    }
    verifyNoMoreInteractions(mockSearchHandler);
  }

  @Test
  public void testBackupPadsWithDownloadedQueries()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    Warmer warmer6 = new Warmer(remoteBackend, service, index, 6, 0, true);
    List<String> downloadedJson = new ArrayList<>(getTestSearchRequestsAsJsonStrings());
    // Add 2 more distinct queries to get 6 total
    downloadedJson.add(
        "{\"indexName\":\"test_index\",\"query\":{\"functionScoreQuery\":{\"query\":{\"termQuery\":{\"field\":\"field4\"}},\"script\":{\"lang\":\"js\",\"source\":\"3 * 5\"}}}}");
    downloadedJson.add(
        "{\"indexName\":\"test_index\",\"query\":{\"functionScoreQuery\":{\"query\":{\"termQuery\":{\"field\":\"field5\"}},\"script\":{\"lang\":\"js\",\"source\":\"3 * 5\"}}}}");
    remoteBackend.uploadWarmingQueries(service, index, getWarmingBytes(downloadedJson));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    warmer6.warmFromS3(mockIndexState, 0, mockSearchHandler);

    // Add only 2 observed queries (deficit of 4)
    warmer6.maybeAddSearchRequest(
        SearchRequest.newBuilder().setIndexName(index).setTopHits(1).build(), 0, null);
    warmer6.maybeAddSearchRequest(
        SearchRequest.newBuilder().setIndexName(index).setTopHits(2).build(), 0, null);

    warmer6.backupWarmingQueriesToS3(service);

    List<String> backedUpLines = readBackedUpLines();
    Assertions.assertThat(backedUpLines).hasSize(6);
  }

  @Test
  public void testBackupNoPaddingWhenFull()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    Warmer warmerWithPad = new Warmer(remoteBackend, service, index, 4, 0, true);
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    warmerWithPad.warmFromS3(mockIndexState, 0, mockSearchHandler);

    // Fill reservoir completely
    getTestSearchRequests().forEach(r -> warmerWithPad.maybeAddSearchRequest(r, 0, null));

    warmerWithPad.backupWarmingQueriesToS3(service);

    List<String> backedUpLines = readBackedUpLines();
    // Only the 4 observed queries, no padding needed
    Assertions.assertThat(backedUpLines).hasSize(4);
    Assertions.assertThat(backedUpLines).containsAll(getTestSearchRequestsAsJsonStrings());
  }

  @Test
  public void testBackupNoPaddingWhenNoDownload() throws IOException {
    // Never call warmFromS3
    warmer.maybeAddSearchRequest(
        SearchRequest.newBuilder().setIndexName(index).setTopHits(1).build(), 0, null);
    warmer.maybeAddSearchRequest(
        SearchRequest.newBuilder().setIndexName(index).setTopHits(2).build(), 0, null);

    warmer.backupWarmingQueriesToS3(service);

    List<String> backedUpLines = readBackedUpLines();
    Assertions.assertThat(backedUpLines).hasSize(2);
  }

  @Test
  public void testBackupNoPaddingWhenFlagDisabled()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    // padWithDownloadedRequests defaults to false
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    warmer.warmFromS3(mockIndexState, 0, mockSearchHandler);

    // Only 1 observed query -- would be padded if flag were enabled
    warmer.maybeAddSearchRequest(
        SearchRequest.newBuilder().setIndexName(index).setTopHits(1).build(), 0, null);

    warmer.backupWarmingQueriesToS3(service);

    List<String> backedUpLines = readBackedUpLines();
    // No padding: only the 1 observed query
    Assertions.assertThat(backedUpLines).hasSize(1);
  }

  @Test
  public void testBackupPaddingWithFewerDownloadedThanDeficit()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    Warmer warmer6 = new Warmer(remoteBackend, service, index, 6, 0, true);
    // Only 2 downloaded, deficit will be 5 when 1 observed
    List<String> downloadedJson =
        List.of(
            getTestSearchRequestsAsJsonStrings().get(0),
            getTestSearchRequestsAsJsonStrings().get(1));
    remoteBackend.uploadWarmingQueries(service, index, getWarmingBytes(downloadedJson));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    warmer6.warmFromS3(mockIndexState, 0, mockSearchHandler);

    warmer6.maybeAddSearchRequest(
        SearchRequest.newBuilder().setIndexName(index).setTopHits(1).build(), 0, null);

    warmer6.backupWarmingQueriesToS3(service);

    List<String> backedUpLines = readBackedUpLines();
    // 2 downloaded + 1 observed = 3 total (less than maxWarmingQueries=6)
    Assertions.assertThat(backedUpLines).hasSize(3);
  }

  @Test
  public void testBackupPaddingOrderIsCorrect()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    Warmer warmerWithPad = new Warmer(remoteBackend, service, index, 4, 0, true);
    // Upload 4 queries [A, B, C, D]
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    warmerWithPad.warmFromS3(mockIndexState, 0, mockSearchHandler);

    // Add 1 observed query [E] -- distinct from the downloaded ones
    SearchRequest observedRequest =
        SearchRequest.newBuilder().setIndexName(index).setTopHits(99).build();
    warmerWithPad.maybeAddSearchRequest(observedRequest, 0, null);

    warmerWithPad.backupWarmingQueriesToS3(service);

    List<String> backedUpLines = readBackedUpLines();
    // Should be [B, C, D, E] -- last 3 from downloaded prepended, then the 1 observed
    Assertions.assertThat(backedUpLines).hasSize(4);
    // Observed query appears last
    String observedJson =
        JsonFormat.printer().omittingInsignificantWhitespace().print(observedRequest);
    Assertions.assertThat(backedUpLines.get(3)).isEqualTo(observedJson);
    // First 3 are the last 3 downloaded queries (B, C, D = indices 1, 2, 3)
    List<String> expectedDownloaded = getTestSearchRequestsAsJsonStrings();
    Assertions.assertThat(backedUpLines.get(0)).isEqualTo(expectedDownloaded.get(1));
    Assertions.assertThat(backedUpLines.get(1)).isEqualTo(expectedDownloaded.get(2));
    Assertions.assertThat(backedUpLines.get(2)).isEqualTo(expectedDownloaded.get(3));
  }

  @Test
  public void testBackupAllFromDownloadedWhenNoObserved()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    Warmer warmerWithPad = new Warmer(remoteBackend, service, index, 4, 0, true);
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    warmerWithPad.warmFromS3(mockIndexState, 0, mockSearchHandler);

    // No observed queries added
    warmerWithPad.backupWarmingQueriesToS3(service);

    List<String> backedUpLines = readBackedUpLines();
    Assertions.assertThat(backedUpLines).hasSize(4);
    Assertions.assertThat(backedUpLines)
        .containsExactlyElementsOf(getTestSearchRequestsAsJsonStrings());
  }

  @Test
  public void testCollectionFilterSkipsPathologicalQueries() throws IOException {
    Warmer filteringWarmer =
        new Warmer(
            remoteBackend,
            service,
            index,
            4,
            0,
            false,
            new WarmingQueryFilter(index, true, 100, 0, 0),
            0);
    SearchRequest termRequest = getTestSearchRequests().get(0);
    filteringWarmer.maybeAddSearchRequest(
        SearchRequest.newBuilder().setIndexName(index).build(), 0, null);
    filteringWarmer.maybeAddSearchRequest(
        termRequest.toBuilder().setTopHits(1000).build(), 0, null);
    filteringWarmer.maybeAddSearchRequest(termRequest, 0, null);

    filteringWarmer.backupWarmingQueriesToS3(service);

    List<String> backedUpLines = readBackedUpLines();
    Assertions.assertThat(backedUpLines)
        .containsExactly(getTestSearchRequestsAsJsonStrings().get(0));
  }

  @Test
  public void testBackupFilterAppliesToPadding()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    Warmer filteringWarmer =
        new Warmer(
            remoteBackend,
            service,
            index,
            4,
            0,
            true,
            new WarmingQueryFilter(index, true, 0, 0, 0),
            0);
    List<String> downloadedJson = new ArrayList<>(getTestSearchRequestsAsJsonStrings());
    downloadedJson.set(1, "{\"indexName\":\"test_index\",\"topHits\":10}");
    remoteBackend.uploadWarmingQueries(service, index, getWarmingBytes(downloadedJson));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    filteringWarmer.warmFromS3(mockIndexState, 0, mockSearchHandler);
    filteringWarmer.backupWarmingQueriesToS3(service);

    List<String> expected = getTestSearchRequestsAsJsonStrings();
    Assertions.assertThat(readBackedUpLines())
        .containsExactly(expected.get(0), expected.get(2), expected.get(3));
  }

  @Test
  public void testReplayFilterSkipsStoredPathologicalQueries()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    Warmer filteringWarmer =
        new Warmer(
            remoteBackend,
            service,
            index,
            4,
            0,
            true,
            new WarmingQueryFilter(index, true, 100, 0, 0),
            0);
    // corpus stored before the filter was configured
    List<SearchRequest> testRequests = getTestSearchRequests();
    List<String> storedJson = new ArrayList<>(getTestSearchRequestsAsJsonStrings());
    storedJson.set(1, "{\"indexName\":\"test_index\",\"topHits\":10}");
    storedJson.set(
        2,
        JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .print(testRequests.get(2).toBuilder().setStartHit(990).setTopHits(1000).build()));
    remoteBackend.uploadWarmingQueries(service, index, getWarmingBytes(storedJson));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    double matchAllBefore = replayFilteredCount("match_all");
    double topHitsBefore = replayFilteredCount("top_hits");
    filteringWarmer.warmFromS3(mockIndexState, 0, mockSearchHandler);

    verify(mockSearchHandler).handle(mockIndexState, testRequests.get(0));
    verify(mockSearchHandler).handle(mockIndexState, testRequests.get(3));
    verifyNoMoreInteractions(mockSearchHandler);
    assertEquals(1, replayFilteredCount("match_all") - matchAllBefore, 0);
    assertEquals(1, replayFilteredCount("top_hits") - topHitsBefore, 0);

    // filtered queries are not used for padding
    filteringWarmer.backupWarmingQueriesToS3(service);
    List<String> expected = getTestSearchRequestsAsJsonStrings();
    Assertions.assertThat(readBackedUpLines()).containsExactly(expected.get(0), expected.get(3));
  }

  @Test
  public void testReplayFilterChecksSimplifiedBasicQuery()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    Warmer filteringWarmer =
        new Warmer(
            remoteBackend,
            service,
            index,
            4,
            100,
            true,
            new WarmingQueryFilter(index, true, 0, 0, 0),
            0);
    // simplifying a top level function filter query leaves a match all query
    SearchRequest functionFilterRequest =
        SearchRequest.newBuilder()
            .setIndexName(index)
            .setQuery(
                Query.newBuilder()
                    .setFunctionFilterQuery(
                        FunctionFilterQuery.newBuilder()
                            .setScript(Script.newBuilder().setLang("js").setSource("10"))))
            .build();
    List<String> storedJson =
        List.of(
            JsonFormat.printer().omittingInsignificantWhitespace().print(functionFilterRequest),
            getTestSearchRequestsAsJsonStrings().get(0));
    remoteBackend.uploadWarmingQueries(service, index, getWarmingBytes(storedJson));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    double basicMatchAllBefore = replayFilteredCount("basic_match_all");
    double matchAllBefore = replayFilteredCount("match_all");
    filteringWarmer.warmFromS3(mockIndexState, 0, mockSearchHandler);

    verify(mockSearchHandler).handle(mockIndexState, getTestBasicSearchRequests().get(0));
    verifyNoMoreInteractions(mockSearchHandler);
    assertEquals(1, replayFilteredCount("basic_match_all") - basicMatchAllBefore, 0);
    assertEquals(0, replayFilteredCount("match_all") - matchAllBefore, 0);

    // the original query is not match all, so it may still be used for padding
    filteringWarmer.backupWarmingQueriesToS3(service);
    Assertions.assertThat(readBackedUpLines()).containsExactlyElementsOf(storedJson);
  }

  @Test
  public void testBasicReplayOfVectorAndMultiRetrieverQueries()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    // with every filter enabled, simplified knn and multi retriever queries are still replayed, and
    // not skipped as match all
    Warmer filteringWarmer =
        new Warmer(
            remoteBackend,
            service,
            index,
            4,
            100,
            true,
            new WarmingQueryFilter(index, true, 100, 0, 1000),
            0);
    Query scriptQuery =
        Query.newBuilder()
            .setFunctionScoreQuery(
                FunctionScoreQuery.newBuilder()
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(TermQuery.newBuilder().setField("id").setTextValue("1")))
                    .setScript(Script.newBuilder().setLang("js").setSource("10")))
            .build();
    KnnQuery knnQuery =
        KnnQuery.newBuilder()
            .setField("vector")
            .setK(10)
            .setNumCandidates(100)
            .setFilter(scriptQuery)
            .build();
    SearchRequest knnRequest =
        SearchRequest.newBuilder().setIndexName(index).addKnn(knnQuery).build();
    SearchRequest multiRetrieverRequest =
        SearchRequest.newBuilder()
            .setIndexName(index)
            .setMultiRetriever(
                MultiRetrieverRequest.newBuilder()
                    .addRetrievers(
                        Retriever.newBuilder()
                            .setName("text")
                            .setRescorer(Rescorer.newBuilder().setWindowSize(10))
                            .setTextRetriever(
                                TextRetriever.newBuilder().setQuery(scriptQuery).setTopHits(10)))
                    .addRetrievers(
                        Retriever.newBuilder()
                            .setName("knn")
                            .setKnnRetriever(KnnRetriever.newBuilder().setKnnQuery(knnQuery))))
            .build();
    List<String> storedJson =
        List.of(
            JsonFormat.printer().omittingInsignificantWhitespace().print(knnRequest),
            JsonFormat.printer().omittingInsignificantWhitespace().print(multiRetrieverRequest));
    remoteBackend.uploadWarmingQueries(service, index, getWarmingBytes(storedJson));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    double basicMatchAllBefore = replayFilteredCount("basic_match_all");
    filteringWarmer.warmFromS3(mockIndexState, 0, mockSearchHandler);

    verify(mockSearchHandler)
        .handle(
            mockIndexState, WarmingUtils.simplifySearchRequestForWarming(knnRequest.toBuilder()));
    verify(mockSearchHandler)
        .handle(
            mockIndexState,
            WarmingUtils.simplifySearchRequestForWarming(multiRetrieverRequest.toBuilder()));
    verifyNoMoreInteractions(mockSearchHandler);
    assertEquals(0, replayFilteredCount("basic_match_all") - basicMatchAllBefore, 0);

    filteringWarmer.backupWarmingQueriesToS3(service);
    Assertions.assertThat(readBackedUpLines()).containsExactlyElementsOf(storedJson);
  }

  @Test
  public void testApplyQueryTimeout() {
    IndexState mockIndexState = mock(IndexState.class);
    SearchRequest.Builder builder = SearchRequest.newBuilder();
    Warmer.applyQueryTimeout(builder, 2500, mockIndexState);
    assertEquals(2.5, builder.getTimeoutSec(), 0);
    assertEquals(Warmer.REPLAY_TIMEOUT_CHECK_EVERY, builder.getTimeoutCheckEvery());

    // a shorter timeout and explicit check interval from the original query are kept
    builder = SearchRequest.newBuilder().setTimeoutSec(0.5).setTimeoutCheckEvery(10);
    Warmer.applyQueryTimeout(builder, 2500, mockIndexState);
    assertEquals(0.5, builder.getTimeoutSec(), 0);
    assertEquals(10, builder.getTimeoutCheckEvery());

    builder = SearchRequest.newBuilder().setTimeoutSec(10);
    Warmer.applyQueryTimeout(builder, 2500, mockIndexState);
    assertEquals(2.5, builder.getTimeoutSec(), 0);
  }

  @Test
  public void testApplyQueryTimeout_indexDefaults() {
    IndexState mockIndexState = mock(IndexState.class);
    when(mockIndexState.getDefaultSearchTimeoutSec()).thenReturn(1.0);
    when(mockIndexState.getDefaultSearchTimeoutCheckEvery()).thenReturn(50);
    // the shorter index defaults still apply
    SearchRequest.Builder builder = SearchRequest.newBuilder();
    Warmer.applyQueryTimeout(builder, 2500, mockIndexState);
    assertEquals(0, builder.getTimeoutSec(), 0);
    assertEquals(0, builder.getTimeoutCheckEvery());

    // a longer index default is limited to the replay timeout
    when(mockIndexState.getDefaultSearchTimeoutSec()).thenReturn(10.0);
    builder = SearchRequest.newBuilder();
    Warmer.applyQueryTimeout(builder, 2500, mockIndexState);
    assertEquals(2.5, builder.getTimeoutSec(), 0);
    assertEquals(0, builder.getTimeoutCheckEvery());
  }

  @Test
  public void testUnparsableQueriesSkipped() throws Exception {
    Warmer padWarmer = new Warmer(remoteBackend, service, index, 4, 0, true);
    List<String> storedJson = new ArrayList<>(getTestSearchRequestsAsJsonStrings());
    // truncated line
    storedJson.set(1, "{\"indexName\":\"test_index\",\"query\":{");
    // field from a newer version
    storedJson.set(
        2,
        "{\"indexName\":\"test_index\",\"unknownField\":1,\"query\":{\"termQuery\":{\"field\":\"f\"}}}");
    remoteBackend.uploadWarmingQueries(service, index, getWarmingBytes(storedJson));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    double errorBefore = replayCount("full", "error");
    double filteredErrorBefore = replayFilteredCount("error");
    padWarmer.warmFromS3(mockIndexState, 0, mockSearchHandler);

    verify(mockSearchHandler, times(3)).handle(any(IndexState.class), any(SearchRequest.class));
    // a parse failure is not a replayed query, it is only counted as filtered
    assertEquals(0, replayCount("full", "error") - errorBefore, 0);
    assertEquals(1, replayFilteredCount("error") - filteredErrorBefore, 0);

    // the unparsable query is not used for padding
    padWarmer.backupWarmingQueriesToS3(service);
    Assertions.assertThat(readBackedUpLines()).hasSize(3);
  }

  @Test
  public void testReplayUsesQueryTimeout()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    Warmer timeoutWarmer = newTimeoutWarmer(2000);
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    timeoutWarmer.warmFromS3(mockIndexState, 0, mockSearchHandler);

    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockSearchHandler, times(4)).handle(any(IndexState.class), captor.capture());
    for (SearchRequest replayed : captor.getAllValues()) {
      assertEquals(2.0, replayed.getTimeoutSec(), 0);
      assertEquals(Warmer.REPLAY_TIMEOUT_CHECK_EVERY, replayed.getTimeoutCheckEvery());
    }
  }

  @Test
  public void testSlowAndFailedReplayQueriesNotUsedForPadding() throws Exception {
    Warmer timeoutWarmer = newTimeoutWarmer(100);
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    List<SearchRequest> requests = getTestSearchRequests();
    // slow query
    when(mockSearchHandler.handle(
            any(IndexState.class), argThat((SearchRequest r) -> sameQuery(r, requests.get(0)))))
        .thenAnswer(
            invocation -> {
              Thread.sleep(300);
              return SearchResponse.getDefaultInstance();
            });
    // query that failed
    when(mockSearchHandler.handle(
            any(IndexState.class), argThat((SearchRequest r) -> sameQuery(r, requests.get(1)))))
        .thenThrow(new SearchHandler.SearchHandlerException("bad query"));
    // query that hit the collection timeout
    when(mockSearchHandler.handle(
            any(IndexState.class), argThat((SearchRequest r) -> sameQuery(r, requests.get(2)))))
        .thenReturn(SearchResponse.newBuilder().setHitTimeout(true).build());
    when(mockSearchHandler.handle(
            any(IndexState.class), argThat((SearchRequest r) -> sameQuery(r, requests.get(3)))))
        .thenReturn(SearchResponse.getDefaultInstance());

    // failures must not stop warming
    timeoutWarmer.warmFromS3(mockIndexState, 0, mockSearchHandler);
    verify(mockSearchHandler, times(4)).handle(any(IndexState.class), any(SearchRequest.class));

    timeoutWarmer.backupWarmingQueriesToS3(service);
    Assertions.assertThat(readBackedUpLines())
        .containsExactly(getTestSearchRequestsAsJsonStrings().get(3));
  }

  @Test
  public void testReplayQueryMetrics() throws Exception {
    Warmer timeoutWarmer = newTimeoutWarmer(100);
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    List<SearchRequest> requests = getTestSearchRequests();
    // slow query
    when(mockSearchHandler.handle(
            any(IndexState.class), argThat((SearchRequest r) -> sameQuery(r, requests.get(0)))))
        .thenAnswer(
            invocation -> {
              Thread.sleep(300);
              return SearchResponse.getDefaultInstance();
            });
    // query that failed
    when(mockSearchHandler.handle(
            any(IndexState.class), argThat((SearchRequest r) -> sameQuery(r, requests.get(1)))))
        .thenThrow(new SearchHandler.SearchHandlerException("bad query"));
    // query that hit the collection timeout
    when(mockSearchHandler.handle(
            any(IndexState.class), argThat((SearchRequest r) -> sameQuery(r, requests.get(2)))))
        .thenReturn(SearchResponse.newBuilder().setHitTimeout(true).build());
    when(mockSearchHandler.handle(
            any(IndexState.class), argThat((SearchRequest r) -> sameQuery(r, requests.get(3)))))
        .thenReturn(SearchResponse.getDefaultInstance());

    double successBefore = replayCount("full", "success");
    double errorBefore = replayCount("full", "error");
    double timeoutBefore = replayCount("full", "timeout");
    long latencyBefore = replayLatencyCount("full");

    timeoutWarmer.warmFromS3(mockIndexState, 0, mockSearchHandler);

    assertEquals(1, replayCount("full", "success") - successBefore, 0);
    assertEquals(1, replayCount("full", "error") - errorBefore, 0);
    assertEquals(2, replayCount("full", "timeout") - timeoutBefore, 0);
    assertEquals(4, replayLatencyCount("full") - latencyBefore);
  }

  private double replayCount(String type, String outcome) {
    return WarmingMetrics.warmingQueryCount.labelValues(index, type, outcome).get();
  }

  private double replayFilteredCount(String reason) {
    return WarmingMetrics.warmingQueriesFilteredCount.labelValues(index, "replay", reason).get();
  }

  private long replayLatencyCount(String type) {
    return WarmingMetrics.warmingQueryLatency.collect().getDataPoints().stream()
        .filter(
            dataPoint ->
                index.equals(dataPoint.getLabels().get("index"))
                    && type.equals(dataPoint.getLabels().get("type")))
        .mapToLong(HistogramDataPointSnapshot::getCount)
        .sum();
  }

  @Test
  public void testMaxWarmingDuration() throws Exception {
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    when(mockSearchHandler.handle(any(IndexState.class), any(SearchRequest.class)))
        .thenAnswer(
            invocation -> {
              Thread.sleep(200);
              return SearchResponse.getDefaultInstance();
            });

    warmer.warmFromS3(mockIndexState, 0, mockSearchHandler, 300, new AtomicBoolean());

    // queries started at 0ms and 200ms, the cap is reached before the third
    verify(mockSearchHandler, atLeastOnce())
        .handle(any(IndexState.class), any(SearchRequest.class));
    Assertions.assertThat(mockingDetails(mockSearchHandler).getInvocations().size()).isLessThan(4);
  }

  @Test
  public void testMaxWarmingDurationStillPads() throws Exception {
    Warmer padWarmer = new Warmer(remoteBackend, service, index, 4, 0, true);
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    when(mockSearchHandler.handle(any(IndexState.class), any(SearchRequest.class)))
        .thenAnswer(
            invocation -> {
              Thread.sleep(200);
              return SearchResponse.getDefaultInstance();
            });

    padWarmer.warmFromS3(mockIndexState, 0, mockSearchHandler, 300, new AtomicBoolean());
    Assertions.assertThat(mockingDetails(mockSearchHandler).getInvocations().size()).isLessThan(4);

    // queries not replayed because of the cap are still used for padding
    padWarmer.backupWarmingQueriesToS3(service);
    Assertions.assertThat(readBackedUpLines())
        .containsExactlyElementsOf(getTestSearchRequestsAsJsonStrings());
  }

  @Test
  public void testStopRequestedStopsNewQueries() throws Exception {
    Warmer padWarmer = new Warmer(remoteBackend, service, index, 4, 0, true);
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    AtomicBoolean stopRequested = new AtomicBoolean();
    // the caller stops waiting while the first query is running
    when(mockSearchHandler.handle(any(IndexState.class), any(SearchRequest.class)))
        .thenAnswer(
            invocation -> {
              stopRequested.set(true);
              return SearchResponse.getDefaultInstance();
            });

    // no max duration of its own, only the stop signal
    padWarmer.warmFromS3(mockIndexState, 0, mockSearchHandler, 0, stopRequested);

    verify(mockSearchHandler, times(1)).handle(any(IndexState.class), any(SearchRequest.class));
    // queries that were not started are still used for padding
    padWarmer.backupWarmingQueriesToS3(service);
    Assertions.assertThat(readBackedUpLines())
        .containsExactlyElementsOf(getTestSearchRequestsAsJsonStrings());
  }

  @Test
  public void testRejectedQueriesDoNotAdvanceSampler() throws IOException {
    Warmer filteringWarmer =
        new Warmer(
            remoteBackend,
            service,
            index,
            4,
            0,
            false,
            new WarmingQueryFilter(index, false, 100, 0, 0),
            0);
    // many rejected queries first, as from a bulk export
    SearchRequest rejected =
        SearchRequest.newBuilder().setIndexName(index).setTopHits(1000).build();
    for (int i = 0; i < 10000; ++i) {
      filteringWarmer.maybeAddSearchRequest(rejected, 0, null);
    }
    // if rejected queries advanced the sampler, these would almost never be sampled
    List<SearchRequest> accepted = getTestSearchRequests();
    accepted.forEach(r -> filteringWarmer.maybeAddSearchRequest(r, 0, null));

    filteringWarmer.backupWarmingQueriesToS3(service);
    Assertions.assertThat(readBackedUpLines())
        .containsExactlyElementsOf(getTestSearchRequestsAsJsonStrings());
  }

  @Test
  public void testQueriesStillRunningNotUsedForPadding() throws Exception {
    // replay waits the minimum wait plus the query timeout for running queries
    Warmer padWarmer =
        new Warmer(
            remoteBackend, service, index, 4, 0, true, WarmingQueryFilter.acceptAll(index), 100);
    padWarmer.setMinReplayWaitMs(100);
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    List<SearchRequest> requests = getTestSearchRequests();
    CountDownLatch release = new CountDownLatch(1);
    when(mockSearchHandler.handle(any(IndexState.class), any(SearchRequest.class)))
        .thenAnswer(
            invocation -> {
              SearchRequest r = invocation.getArgument(1);
              if (sameQuery(r, requests.get(0))) {
                // still running when warming stops waiting
                release.await();
              }
              return SearchResponse.getDefaultInstance();
            });
    try {
      // one pool thread runs the first query, the caller runs the rest
      long startNs = System.nanoTime();
      padWarmer.warmFromS3(mockIndexState, 2, mockSearchHandler, 0, new AtomicBoolean());
      Assertions.assertThat(System.nanoTime() - startNs)
          .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(200));
    } finally {
      release.countDown();
    }

    // the result of the running query is unknown, so it is not used for padding
    padWarmer.backupWarmingQueriesToS3(service);
    List<String> expected = getTestSearchRequestsAsJsonStrings();
    Assertions.assertThat(readBackedUpLines())
        .containsExactly(expected.get(1), expected.get(2), expected.get(3));
  }

  private Warmer newTimeoutWarmer(int queryTimeoutMs) {
    return new Warmer(
        remoteBackend,
        service,
        index,
        4,
        0,
        true,
        WarmingQueryFilter.acceptAll(index),
        queryTimeoutMs);
  }

  private static boolean sameQuery(SearchRequest request, SearchRequest expected) {
    return request != null && request.getQuery().equals(expected.getQuery());
  }

  private List<String> readBackedUpLines() throws IOException {
    InputStream queriesStream = remoteBackend.downloadWarmingQueries(service, index);
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(queriesStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    }
    return lines;
  }

  private byte[] getWarmingBytes(List<String> queryStrings) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (OutputStreamWriter writer =
        new OutputStreamWriter(byteArrayOutputStream, StandardCharsets.UTF_8)) {
      for (String line : queryStrings) {
        writer.write(line);
        writer.write("\n");
      }
    }
    return byteArrayOutputStream.toByteArray();
  }

  private List<SearchRequest> getTestSearchRequests() {
    List<SearchRequest> testRequests = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      SearchRequest searchRequest =
          SearchRequest.newBuilder()
              .setIndexName(index)
              .setQuery(
                  Query.newBuilder()
                      .setFunctionScoreQuery(
                          FunctionScoreQuery.newBuilder()
                              .setQuery(
                                  Query.newBuilder()
                                      .setTermQuery(TermQuery.newBuilder().setField("field" + i)))
                              .setScript(Script.newBuilder().setLang("js").setSource("3 * 5"))))
              .build();
      testRequests.add(searchRequest);
    }
    return testRequests;
  }

  private List<SearchRequest> getTestBasicSearchRequests() {
    List<SearchRequest> testRequests = new ArrayList<>();
    int i = 0;
    for (; i < 2; i++) {
      SearchRequest searchRequest =
          SearchRequest.newBuilder()
              .setIndexName(index)
              .setQuery(
                  Query.newBuilder().setTermQuery(TermQuery.newBuilder().setField("field" + i)))
              .build();
      testRequests.add(searchRequest);
    }

    for (; i < 4; i++) {
      SearchRequest searchRequest =
          SearchRequest.newBuilder()
              .setIndexName(index)
              .setQuery(
                  Query.newBuilder()
                      .setFunctionScoreQuery(
                          FunctionScoreQuery.newBuilder()
                              .setQuery(
                                  Query.newBuilder()
                                      .setTermQuery(TermQuery.newBuilder().setField("field" + i)))
                              .setScript(Script.newBuilder().setLang("js").setSource("3 * 5"))))
              .build();
      testRequests.add(searchRequest);
    }

    return testRequests;
  }

  private List<String> getTestSearchRequestsAsJsonStrings() {
    return List.of(
        "{\"indexName\":\"test_index\",\"query\":{\"functionScoreQuery\":{\"query\":{\"termQuery\":{\"field\":\"field0\"}},\"script\":{\"lang\":\"js\",\"source\":\"3 * 5\"}}}}",
        "{\"indexName\":\"test_index\",\"query\":{\"functionScoreQuery\":{\"query\":{\"termQuery\":{\"field\":\"field1\"}},\"script\":{\"lang\":\"js\",\"source\":\"3 * 5\"}}}}",
        "{\"indexName\":\"test_index\",\"query\":{\"functionScoreQuery\":{\"query\":{\"termQuery\":{\"field\":\"field2\"}},\"script\":{\"lang\":\"js\",\"source\":\"3 * 5\"}}}}",
        "{\"indexName\":\"test_index\",\"query\":{\"functionScoreQuery\":{\"query\":{\"termQuery\":{\"field\":\"field3\"}},\"script\":{\"lang\":\"js\",\"source\":\"3 * 5\"}}}}");
  }

  @Test
  public void testWarmFromRequests() throws Exception {
    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    List<SearchRequest> requests = getTestSearchRequests();

    warmer.warmFromRequests(mockIndexState, 0, mockSearchHandler, requests);

    for (SearchRequest request : requests) {
      verify(mockSearchHandler).handle(mockIndexState, request);
    }
    verifyNoMoreInteractions(mockSearchHandler);
  }

  @Test
  public void testWarmFromRequests_parallel() throws Exception {
    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    List<SearchRequest> requests = getTestSearchRequests();

    warmer.warmFromRequests(mockIndexState, 3, mockSearchHandler, requests);

    for (SearchRequest request : requests) {
      verify(mockSearchHandler).handle(mockIndexState, request);
    }
    verifyNoMoreInteractions(mockSearchHandler);
  }

  @Test
  public void testWarmFromRequests_schemaSettingsAndKindMetrics() throws Exception {
    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    List<SearchRequest> requests = getTestSearchRequests();
    double filterBefore =
        WarmingMetrics.warmingSchemaQueryCount.labelValues(index, "filter", "success").get();
    warmer.setSchemaQuerySettings(
        new WarmerConfig.SchemaQuerySettings(3, 1500, SchemaWarmingQueryGenerator.Limits.DEFAULT));

    warmer.warmFromRequests(mockIndexState, 0, mockSearchHandler, requests);

    // schema queries get the schema query timeout, the warmer's own timeout is not set
    for (SearchRequest request : requests) {
      verify(mockSearchHandler)
          .handle(
              org.mockito.ArgumentMatchers.eq(mockIndexState),
              org.mockito.ArgumentMatchers.argThat(
                  r ->
                      r.getQuery().equals(request.getQuery())
                          && r.getTimeoutSec() == 1.5
                          && r.getTimeoutCheckEvery() == Warmer.REPLAY_TIMEOUT_CHECK_EVERY));
    }
    // the test queries are function score queries over a term query: filter kind
    assertEquals(
        filterBefore + requests.size(),
        WarmingMetrics.warmingSchemaQueryCount.labelValues(index, "filter", "success").get(),
        0);
  }

  @Test
  public void testFormatKindStats() {
    Warmer.KindStats script = new Warmer.KindStats();
    script.record(TimeUnit.MILLISECONDS.toNanos(3000), true);
    script.record(TimeUnit.MILLISECONDS.toNanos(1000), false);
    Warmer.KindStats filter = new Warmer.KindStats();
    filter.record(TimeUnit.MILLISECONDS.toNanos(10), false);
    java.util.Map<String, Warmer.KindStats> stats = new java.util.LinkedHashMap<>();
    stats.put("filter", filter);
    stats.put("script", script);
    // most costly kind first
    assertEquals(
        "script=2/4.0s/2000ms/3000ms/1, filter=1/0.0s/10ms/10ms/0", Warmer.formatKindStats(stats));
  }

  @Test
  public void testWarmFromRequests_failureDoesNotStopWarming() throws Exception {
    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    List<SearchRequest> requests = getTestSearchRequests();
    when(mockSearchHandler.handle(mockIndexState, requests.get(0)))
        .thenThrow(new IllegalArgumentException("bad query"));

    warmer.warmFromRequests(mockIndexState, 0, mockSearchHandler, requests);

    for (SearchRequest request : requests) {
      verify(mockSearchHandler).handle(mockIndexState, request);
    }
  }

  private static List<String> chunkLines(List<byte[]> chunks) {
    List<String> lines = new ArrayList<>();
    for (byte[] chunk : chunks) {
      String content = new String(chunk, StandardCharsets.UTF_8);
      // every chunk holds whole queries
      assertEquals('\n', content.charAt(content.length() - 1));
      lines.addAll(List.of(content.substring(0, content.length() - 1).split("\n")));
    }
    return lines;
  }

  @Test
  public void testWriteWarmingCorpus() {
    getTestSearchRequests().forEach(r -> warmer.maybeAddSearchRequest(r, 0, null));
    List<byte[]> chunks = new ArrayList<>();

    int written = warmer.writeWarmingCorpus(0, 0, 1024 * 1024, chunks::add);

    assertEquals(4, written);
    assertEquals(1, chunks.size());
    Assertions.assertThat(chunkLines(chunks))
        .containsExactlyInAnyOrderElementsOf(getTestSearchRequestsAsJsonStrings());
  }

  @Test
  public void testWriteWarmingCorpus_empty() {
    List<byte[]> chunks = new ArrayList<>();
    assertEquals(0, warmer.writeWarmingCorpus(0, 0, 1024, chunks::add));
    assertEquals(0, chunks.size());
  }

  @Test
  public void testWriteWarmingCorpus_maxQueries() {
    getTestSearchRequests().forEach(r -> warmer.maybeAddSearchRequest(r, 0, null));
    List<byte[]> chunks = new ArrayList<>();

    assertEquals(2, warmer.writeWarmingCorpus(2, 0, 1024 * 1024, chunks::add));
    assertEquals(2, chunkLines(chunks).size());
  }

  @Test
  public void testWriteWarmingCorpus_maxBytes() {
    getTestSearchRequests().forEach(r -> warmer.maybeAddSearchRequest(r, 0, null));
    int lineBytes = getTestSearchRequestsAsJsonStrings().get(0).length() + 1;
    List<byte[]> chunks = new ArrayList<>();

    // room for two whole queries and part of a third, the third is not sent
    int written = warmer.writeWarmingCorpus(0, 2L * lineBytes + 10, 1024 * 1024, chunks::add);

    assertEquals(2, written);
    assertEquals(2, chunkLines(chunks).size());
  }

  @Test
  public void testWriteWarmingCorpus_chunks() {
    getTestSearchRequests().forEach(r -> warmer.maybeAddSearchRequest(r, 0, null));
    int lineBytes = getTestSearchRequestsAsJsonStrings().get(0).length() + 1;
    List<byte[]> chunks = new ArrayList<>();

    // two queries fit in a chunk
    warmer.writeWarmingCorpus(0, 0, 2 * lineBytes, chunks::add);
    assertEquals(2, chunks.size());
    Assertions.assertThat(chunkLines(chunks))
        .containsExactlyInAnyOrderElementsOf(getTestSearchRequestsAsJsonStrings());

    // a query larger than the chunk size is its own chunk
    chunks.clear();
    warmer.writeWarmingCorpus(0, 0, 1, chunks::add);
    assertEquals(4, chunks.size());
  }

  @Test
  public void testWarmFromPeer() throws Exception {
    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    byte[] peerBytes = getWarmingBytes(getTestSearchRequestsAsJsonStrings());
    double successBefore = peerFetchCount(Warmer.PEER_FETCH_SUCCESS);

    warmer.warmFromPeer(
        mockIndexState,
        0,
        mockSearchHandler,
        () -> Optional.of(new PeerWarmingQuerySource.PeerWarmingQueries("peer", peerBytes)),
        WarmerConfig.WarmingStrategy.S3,
        10,
        0,
        new AtomicBoolean());

    for (SearchRequest testRequest : getTestSearchRequests()) {
      verify(mockSearchHandler).handle(mockIndexState, testRequest);
    }
    verifyNoMoreInteractions(mockSearchHandler);
    assertEquals(1, peerFetchCount(Warmer.PEER_FETCH_SUCCESS) - successBefore, 0);
  }

  @Test
  public void testWarmFromPeer_padsBackup() throws Exception {
    Warmer paddingWarmer =
        new Warmer(
            remoteBackend, service, index, 4, 0, true, WarmingQueryFilter.acceptAll(index), 0);
    byte[] peerBytes = getWarmingBytes(getTestSearchRequestsAsJsonStrings());

    paddingWarmer.warmFromPeer(
        mock(IndexState.class),
        0,
        mock(SearchHandler.class),
        () -> Optional.of(new PeerWarmingQuerySource.PeerWarmingQueries("peer", peerBytes)),
        WarmerConfig.WarmingStrategy.NONE,
        10,
        0,
        new AtomicBoolean());

    // a replica warmed from a peer can itself serve the peer's queries before it has traffic
    Assertions.assertThat(paddingWarmer.getWarmingCorpus())
        .containsExactlyElementsOf(getTestSearchRequests());
  }

  @Test
  public void testWarmFromTemplate_peerSeeds() throws Exception {
    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    org.mockito.Mockito.when(
            mockSearchHandler.handle(
                org.mockito.ArgumentMatchers.eq(mockIndexState),
                org.mockito.ArgumentMatchers.any(SearchRequest.class)))
        .thenReturn(SearchResponse.getDefaultInstance());
    byte[] peerBytes = getWarmingBytes(getTestSearchRequestsAsJsonStrings());

    warmer.warmFromTemplate(
        mockIndexState,
        0,
        mockSearchHandler,
        () -> Optional.of(new PeerWarmingQuerySource.PeerWarmingQueries("peer", peerBytes)),
        100,
        10,
        0,
        new AtomicBoolean());

    // generated queries were replayed
    verify(mockSearchHandler, org.mockito.Mockito.atLeastOnce())
        .handle(
            org.mockito.ArgumentMatchers.eq(mockIndexState),
            org.mockito.ArgumentMatchers.any(SearchRequest.class));
  }

  @Test
  public void testWarmFromTemplate_s3Seeds() throws Exception {
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));
    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    org.mockito.Mockito.when(
            mockSearchHandler.handle(
                org.mockito.ArgumentMatchers.eq(mockIndexState),
                org.mockito.ArgumentMatchers.any(SearchRequest.class)))
        .thenReturn(SearchResponse.getDefaultInstance());

    warmer.warmFromTemplate(
        mockIndexState, 0, mockSearchHandler, Optional::empty, 100, 10, 0, new AtomicBoolean());

    verify(mockSearchHandler, org.mockito.Mockito.atLeastOnce())
        .handle(
            org.mockito.ArgumentMatchers.eq(mockIndexState),
            org.mockito.ArgumentMatchers.any(SearchRequest.class));
  }

  private static final WarmerConfig.TemplateLibrarySettings LIBRARY =
      new WarmerConfig.TemplateLibrarySettings(true, 0.95, 1, 2, 0, 0.5, 0.5, 1);

  private SearchHandler respondingSearchHandler(IndexState indexState) throws Exception {
    SearchHandler handler = mock(SearchHandler.class);
    org.mockito.Mockito.when(
            handler.handle(
                org.mockito.ArgumentMatchers.eq(indexState),
                org.mockito.ArgumentMatchers.any(SearchRequest.class)))
        .thenReturn(SearchResponse.getDefaultInstance());
    return handler;
  }

  @Test
  public void testTemplateLibrary_learnOnceThenLoad() throws Exception {
    IndexState mockIndexState = mock(IndexState.class);
    byte[] peerBytes = getWarmingBytes(getTestSearchRequestsAsJsonStrings());

    // first replica: no library yet, learns from peer seeds and stores the library
    Warmer first = new Warmer(remoteBackend, service, index, 4);
    first.enableTemplateLibrary(LIBRARY);
    first.warmFromTemplate(
        mockIndexState,
        0,
        respondingSearchHandler(mockIndexState),
        () -> Optional.of(new PeerWarmingQuerySource.PeerWarmingQueries("peer", peerBytes)),
        100,
        10,
        0,
        new AtomicBoolean());
    assertTrue(
        remoteBackend.exists(service, index, RemoteBackend.IndexResourceType.WARMING_TEMPLATES));
    int learnedTemplates = first.getTemplateLibrary().getNumTemplates();
    assertTrue(learnedTemplates > 0);

    // second replica: no peer and no S3 corpus, warms from the stored library
    Warmer second = new Warmer(remoteBackend, service, index, 4);
    second.enableTemplateLibrary(LIBRARY);
    SearchHandler secondHandler = respondingSearchHandler(mockIndexState);
    second.warmFromTemplate(
        mockIndexState, 0, secondHandler, Optional::empty, 100, 10, 0, new AtomicBoolean());
    assertEquals(learnedTemplates, second.getTemplateLibrary().getNumTemplates());
    verify(secondHandler, org.mockito.Mockito.atLeastOnce())
        .handle(
            org.mockito.ArgumentMatchers.eq(mockIndexState),
            org.mockito.ArgumentMatchers.any(SearchRequest.class));
  }

  @Test
  public void testTemplateLibrary_relearnMergesAndStores() throws Exception {
    Warmer libraryWarmer = new Warmer(remoteBackend, service, index, 4);
    libraryWarmer.enableTemplateLibrary(
        new WarmerConfig.TemplateLibrarySettings(true, 0.95, 1, 2, 0, 0.5, 0.5, 100));
    getTestSearchRequests().forEach(r -> libraryWarmer.maybeAddSearchRequest(r, 0, null));
    // too few collected queries
    assertNull(libraryWarmer.relearnTemplates(0.5));

    Warmer enoughQueriesWarmer = new Warmer(remoteBackend, service, index, 4);
    enoughQueriesWarmer.enableTemplateLibrary(LIBRARY);
    getTestSearchRequests().forEach(r -> enoughQueriesWarmer.maybeAddSearchRequest(r, 0, null));
    Set<String> keys = enoughQueriesWarmer.relearnTemplates(0.5);
    assertNotNull(keys);
    assertFalse(keys.isEmpty());
    assertTrue(
        remoteBackend.exists(service, index, RemoteBackend.IndexResourceType.WARMING_TEMPLATES));
  }

  @Test
  public void testTemplateLibrary_learnsFromLiveTrafficWhenNothingElse() throws Exception {
    // no library, no peer, no S3 corpus: startup falls back, then the drift monitor learns the
    // library from the first collected live queries
    Warmer libraryWarmer = new Warmer(remoteBackend, service, index, 4);
    libraryWarmer.enableTemplateLibrary(LIBRARY);
    libraryWarmer.warmFromTemplate(
        mock(IndexState.class),
        0,
        mock(SearchHandler.class),
        Optional::empty,
        100,
        10,
        0,
        new AtomicBoolean());
    assertNull(libraryWarmer.getTemplateLibrary());

    for (SearchRequest r : getTestSearchRequests()) {
      libraryWarmer.maybeAddSearchRequest(r, 0, null);
    }
    long deadline = System.currentTimeMillis() + 10_000;
    while (libraryWarmer.getTemplateLibrary() == null && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }
    assertNotNull(libraryWarmer.getTemplateLibrary());
    assertTrue(
        remoteBackend.exists(service, index, RemoteBackend.IndexResourceType.WARMING_TEMPLATES));
  }

  @Test
  public void testWarmFromTemplate_noSeedsFallsBack() throws Exception {
    SearchHandler mockSearchHandler = mock(SearchHandler.class);

    // no peer and no S3 corpus: falls back to SCHEMA, which can't generate from a mock index and
    // falls back to S3, which has nothing either
    warmer.warmFromTemplate(
        mock(IndexState.class),
        0,
        mockSearchHandler,
        Optional::empty,
        100,
        10,
        0,
        new AtomicBoolean());

    verifyNoMoreInteractions(mockSearchHandler);
  }

  @Test
  public void testWarmFromTemplate_thenSchemaQueries() throws Exception {
    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler handler = respondingSearchHandler(mockIndexState);
    byte[] peerBytes = getWarmingBytes(getTestSearchRequestsAsJsonStrings());
    SearchRequest schemaRequest = SearchRequest.newBuilder().setIndexName("schema_only").build();
    Warmer spyWarmer = org.mockito.Mockito.spy(warmer);
    org.mockito.Mockito.doReturn(List.of(schemaRequest, schemaRequest))
        .when(spyWarmer)
        .generateSchemaQueries(mockIndexState, 7);

    spyWarmer.warmFromTemplate(
        mockIndexState,
        0,
        handler,
        () -> Optional.of(new PeerWarmingQuerySource.PeerWarmingQueries("peer", peerBytes)),
        100,
        10,
        7,
        0,
        new AtomicBoolean());

    // template queries ran, then the schema queries
    verify(handler, org.mockito.Mockito.atLeastOnce())
        .handle(
            org.mockito.ArgumentMatchers.eq(mockIndexState),
            org.mockito.ArgumentMatchers.argThat(r -> !"schema_only".equals(r.getIndexName())));
    verify(handler, org.mockito.Mockito.times(2))
        .handle(
            org.mockito.ArgumentMatchers.eq(mockIndexState),
            org.mockito.ArgumentMatchers.argThat(r -> "schema_only".equals(r.getIndexName())));
  }

  @Test
  public void testWarmFromTemplate_noSchemaQueriesByDefault() throws Exception {
    IndexState mockIndexState = mock(IndexState.class);
    byte[] peerBytes = getWarmingBytes(getTestSearchRequestsAsJsonStrings());
    Warmer spyWarmer = org.mockito.Mockito.spy(warmer);

    spyWarmer.warmFromTemplate(
        mockIndexState,
        0,
        respondingSearchHandler(mockIndexState),
        () -> Optional.of(new PeerWarmingQuerySource.PeerWarmingQueries("peer", peerBytes)),
        100,
        10,
        0,
        new AtomicBoolean());

    verify(spyWarmer, org.mockito.Mockito.never())
        .generateSchemaQueries(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyInt());
  }

  @Test
  public void testWarmFromTemplate_schemaQueriesStopWhenRequested() throws Exception {
    IndexState mockIndexState = mock(IndexState.class);
    byte[] peerBytes = getWarmingBytes(getTestSearchRequestsAsJsonStrings());
    Warmer spyWarmer = org.mockito.Mockito.spy(warmer);

    // startup stopped waiting: no template query starts, and no schema query is generated
    spyWarmer.warmFromTemplate(
        mockIndexState,
        0,
        respondingSearchHandler(mockIndexState),
        () -> Optional.of(new PeerWarmingQuerySource.PeerWarmingQueries("peer", peerBytes)),
        100,
        10,
        7,
        0,
        new AtomicBoolean(true));

    verify(spyWarmer, org.mockito.Mockito.never())
        .generateSchemaQueries(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyInt());
  }

  @Test
  public void testWarmFromPeer_fallbackToS3() throws Exception {
    remoteBackend.uploadWarmingQueries(
        service, index, getWarmingBytes(getTestSearchRequestsAsJsonStrings()));
    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    double fallbackBefore = peerFetchCount(Warmer.PEER_FETCH_FALLBACK);

    warmer.warmFromPeer(
        mockIndexState,
        0,
        mockSearchHandler,
        Optional::empty,
        WarmerConfig.WarmingStrategy.S3,
        10,
        0,
        new AtomicBoolean());

    for (SearchRequest testRequest : getTestSearchRequests()) {
      verify(mockSearchHandler).handle(mockIndexState, testRequest);
    }
    verifyNoMoreInteractions(mockSearchHandler);
    assertEquals(1, peerFetchCount(Warmer.PEER_FETCH_FALLBACK) - fallbackBefore, 0);
  }

  @Test
  public void testWarmFromPeer_fetchFailureFallsBack() throws Exception {
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    double fallbackBefore = peerFetchCount(Warmer.PEER_FETCH_FALLBACK);

    warmer.warmFromPeer(
        mock(IndexState.class),
        0,
        mockSearchHandler,
        () -> {
          throw new IllegalStateException("peer failure");
        },
        WarmerConfig.WarmingStrategy.NONE,
        10,
        0,
        new AtomicBoolean());

    verifyNoMoreInteractions(mockSearchHandler);
    assertEquals(1, peerFetchCount(Warmer.PEER_FETCH_FALLBACK) - fallbackBefore, 0);
  }

  @Test
  public void testWarmFromPeer_stopRequested() throws Exception {
    SearchHandler mockSearchHandler = mock(SearchHandler.class);
    byte[] peerBytes = getWarmingBytes(getTestSearchRequestsAsJsonStrings());

    warmer.warmFromPeer(
        mock(IndexState.class),
        0,
        mockSearchHandler,
        () -> Optional.of(new PeerWarmingQuerySource.PeerWarmingQueries("peer", peerBytes)),
        WarmerConfig.WarmingStrategy.S3,
        10,
        0,
        new AtomicBoolean(true));

    verifyNoMoreInteractions(mockSearchHandler);
  }

  private double peerFetchCount(String outcome) {
    return WarmingMetrics.warmingPeerFetchCount.labelValues(index, outcome).get();
  }
}
