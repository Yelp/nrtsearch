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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.amazonaws.services.s3.AmazonS3;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.handler.SearchHandler;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
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
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WarmerTest {

  private final String service = "test_service";
  private final String index = "test_index";
  private final String bucketName = "warmer-unittest";
  private RemoteBackend remoteBackend;
  private AmazonS3 s3;
  private Warmer warmer;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  @Rule public final AmazonS3Provider s3Provider = new AmazonS3Provider(bucketName);

  @Before
  public void setup() throws IOException {
    String configStr = "bucketName: " + bucketName;
    NrtsearchConfig config = new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));
    s3 = s3Provider.getAmazonS3();
    remoteBackend = new S3Backend(config, s3);
    warmer = new Warmer(remoteBackend, service, index, 2);
  }

  @Test
  public void testAddSearchRequest_backupWarmingQueriesToS3() throws IOException {
    List<SearchRequest> testRequests = getTestSearchRequests();

    testRequests.forEach(warmer::addSearchRequest);

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
    Warmer warmerWithBasic = new Warmer(remoteBackend, service, index, 2, 30);

    List<String> testSearchRequestsJson = getTestSearchRequestsAsJsonStrings();
    byte[] warmingBytes = getWarmingBytes(testSearchRequestsJson);
    remoteBackend.uploadWarmingQueries(service, "test_index", warmingBytes);

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);

    // nextInt(100) for this seed is: 28, 33, 20, 10
    warmerWithBasic.randomThreadLocal.get().setSeed(1234);
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
    for (int i = 0; i < 2; i++) {
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
    SearchRequest searchRequest =
        SearchRequest.newBuilder()
            .setIndexName(index)
            .setQuery(Query.newBuilder().setTermQuery(TermQuery.newBuilder().setField("field0")))
            .build();
    testRequests.add(searchRequest);

    searchRequest =
        SearchRequest.newBuilder()
            .setIndexName(index)
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setQuery(
                                Query.newBuilder()
                                    .setTermQuery(TermQuery.newBuilder().setField("field1")))
                            .setScript(Script.newBuilder().setLang("js").setSource("3 * 5"))))
            .build();
    testRequests.add(searchRequest);

    return testRequests;
  }

  private List<String> getTestSearchRequestsAsJsonStrings() {
    return List.of(
        "{\"indexName\":\"test_index\",\"query\":{\"functionScoreQuery\":{\"query\":{\"termQuery\":{\"field\":\"field0\"}},\"script\":{\"lang\":\"js\",\"source\":\"3 * 5\"}}}}",
        "{\"indexName\":\"test_index\",\"query\":{\"functionScoreQuery\":{\"query\":{\"termQuery\":{\"field\":\"field1\"}},\"script\":{\"lang\":\"js\",\"source\":\"3 * 5\"}}}}");
  }
}
