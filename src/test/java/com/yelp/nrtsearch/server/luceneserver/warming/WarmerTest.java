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
package com.yelp.nrtsearch.server.luceneserver.warming;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.backup.ArchiverImpl;
import com.yelp.nrtsearch.server.backup.TarImpl;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import io.findify.s3mock.S3Mock;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WarmerTest {

  private final String service = "test_service";
  private final String index = "test_index";
  private final String resource = "test_index_warming_queries";
  private Archiver archiver;
  private AmazonS3 s3;
  private S3Mock api;
  private Warmer warmer;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    Path s3Directory = folder.newFolder("s3").toPath();
    Path archiverDirectory = folder.newFolder("archiver").toPath();

    api = S3Mock.create(8011, s3Directory.toAbsolutePath().toString());
    api.start();
    s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint("http://127.0.0.1:8011");
    String bucketName = "warmer-unittest";
    s3.createBucket(bucketName);

    archiver =
        new ArchiverImpl(
            s3, bucketName, archiverDirectory, new TarImpl(TarImpl.CompressionMode.LZ4));
    warmer = new Warmer(archiver, service, index, 2);
  }

  @After
  public void teardown() {
    s3.shutdown();
    api.shutdown();
  }

  @Test
  public void testAddSearchRequest_backupWarmingQueriesToS3() throws IOException {
    List<SearchRequest> testRequests = getTestSearchRequests();

    testRequests.forEach(warmer::addSearchRequest);

    warmer.backupWarmingQueriesToS3(service);

    Path downloadPath = archiver.download(service, resource);

    Path warmingQueriesDir = downloadPath.resolve("warming_queries");
    Path warmingQueriesFile = warmingQueriesDir.resolve("warming_queries.txt");
    List<String> lines = Files.readAllLines(warmingQueriesFile);
    Assertions.assertThat(lines).containsAll(getTestSearchRequestsAsJsonStrings());
  }

  @Test
  public void testWarmFromS3()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    Path warmingQueriesDir = folder.newFolder("warming_queries").toPath();
    try (BufferedWriter writer =
        Files.newBufferedWriter(warmingQueriesDir.resolve("warming_queries.txt"))) {
      List<String> testSearchRequestsJson = getTestSearchRequestsAsJsonStrings();
      for (String line : testSearchRequestsJson) {
        writer.write(line);
        writer.newLine();
      }
      writer.flush();
    }
    String versionHash =
        archiver.upload(service, resource, warmingQueriesDir, List.of(), List.of(), false);
    archiver.blessVersion(service, resource, versionHash);

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);

    warmer.warmFromS3(mockIndexState, 0, mockSearchHandler);

    for (SearchRequest testRequest : getTestSearchRequests()) {
      verify(mockSearchHandler).handle(mockIndexState, testRequest);
    }
    verifyNoMoreInteractions(mockSearchHandler);
  }

  @Test
  public void testWarmFromS3_multiple()
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    Path warmingQueriesDir = folder.newFolder("warming_queries").toPath();
    try (BufferedWriter writer =
        Files.newBufferedWriter(warmingQueriesDir.resolve("warming_queries.txt"))) {
      List<String> testSearchRequestsJson = getTestSearchRequestsAsJsonStrings();
      for (String line : testSearchRequestsJson) {
        writer.write(line);
        writer.newLine();
      }
      writer.flush();
    }
    String versionHash =
        archiver.upload(service, resource, warmingQueriesDir, List.of(), List.of(), false);
    archiver.blessVersion(service, resource, versionHash);

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
    Path warmingQueriesDir = folder.newFolder("warming_queries").toPath();
    int warmingCountPerQuery = 10;
    try (BufferedWriter writer =
        Files.newBufferedWriter(warmingQueriesDir.resolve("warming_queries.txt"))) {
      List<String> testSearchRequestsJson = getTestSearchRequestsAsJsonStrings();
      List<String> moreTestSearchRequestsJson = new ArrayList<>();
      for (int i = 0; i < warmingCountPerQuery; i++) {
        moreTestSearchRequestsJson.addAll(testSearchRequestsJson);
      }
      for (String line : moreTestSearchRequestsJson) {
        writer.write(line);
        writer.newLine();
      }
      writer.flush();
    }
    String versionHash =
        archiver.upload(service, resource, warmingQueriesDir, List.of(), List.of(), false);
    archiver.blessVersion(service, resource, versionHash);

    IndexState mockIndexState = mock(IndexState.class);
    SearchHandler mockSearchHandler = mock(SearchHandler.class);

    warmer.warmFromS3(mockIndexState, 3, mockSearchHandler);

    for (SearchRequest testRequest : getTestSearchRequests()) {
      verify(mockSearchHandler, times(warmingCountPerQuery)).handle(mockIndexState, testRequest);
    }
    verifyNoMoreInteractions(mockSearchHandler);
  }

  private List<SearchRequest> getTestSearchRequests() {
    List<SearchRequest> testRequests = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      SearchRequest searchRequest =
          SearchRequest.newBuilder()
              .setIndexName(index)
              .setQuery(
                  Query.newBuilder()
                      .setTermQuery(TermQuery.newBuilder().setField("field" + i).build())
                      .build())
              .build();
      testRequests.add(searchRequest);
    }
    return testRequests;
  }

  private List<String> getTestSearchRequestsAsJsonStrings() {
    return List.of(
        "{\"indexName\":\"test_index\",\"query\":{\"termQuery\":{\"field\":\"field0\"}}}",
        "{\"indexName\":\"test_index\",\"query\":{\"termQuery\":{\"field\":\"field1\"}}}");
  }
}
