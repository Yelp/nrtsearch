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
package com.yelp.nrtsearch.test_utils;

import io.findify.s3mock.S3Mock;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * A JUnit {@link org.junit.Rule} for mock S3 tests. It provides a mock S3 client which stores any
 * files in and retrieves from a {@link TemporaryFolder} so that all data added to S3 is deleted
 * after the test.
 *
 * <p>Example of usage:
 *
 * <pre>
 * public static class S3Test {
 *  &#064;Rule
 *  public AmazonS3Provider s3Provider = new AmazonS3Provider("test-bucket");
 *
 *  &#064;Test
 *  public void testUsingS3() throws IOException {
 *      S3Client s3Client = s3Provider.getS3Client();
 *      s3Client.putObject(...);
 *      // ...
 *     }
 * }
 * </pre>
 */
public class AmazonS3Provider extends ExternalResource {

  /** Holds a successfully started S3Mock instance and its base endpoint URL. */
  public record StartedMock(S3Mock api, String endpoint) {}

  private final String bucketName;
  private final TemporaryFolder temporaryFolder;
  private S3Mock api;
  private S3Client s3;
  private S3AsyncClient s3Async;
  private S3TransferManager transferManager;
  private String s3Path;

  /**
   * Starts an S3Mock instance with two-phase readiness probing and restart-on-failure.
   *
   * <p>Handles two failure modes:
   *
   * <ol>
   *   <li>BindException — {@link PortUtils#findAvailablePort()} TOCTOU race; retries on a new port.
   *   <li>Phase 2 exhaustion — Akka accepts connections (Phase 1 ok) but its PUT routing never
   *       becomes functional; shuts down the stuck instance and starts fresh.
   * </ol>
   *
   * @param s3Path file-backend directory for S3Mock
   * @return a {@link StartedMock} with the running instance and its endpoint URL
   * @throws IOException if all 5 startup attempts fail
   */
  public static StartedMock startS3Mock(String s3Path) throws IOException {
    Exception lastException = null;
    for (int attempt = 0; attempt < 5; attempt++) {
      int port = PortUtils.findAvailablePort();
      S3Mock mockApi = new S3Mock.Builder().withPort(port).withFileBackend(s3Path).build();
      try {
        mockApi.start();
      } catch (Exception e) {
        lastException = e;
        continue;
      }
      String endpoint = String.format("http://127.0.0.1:%d", port);
      // Phase 1: wait for Akka's HTTP layer to accept any connection.
      for (int readyAttempt = 0; readyAttempt < 100; readyAttempt++) {
        try {
          HttpURLConnection conn = (HttpURLConnection) new URL(endpoint + "/").openConnection();
          conn.setConnectTimeout(500);
          conn.setReadTimeout(500);
          try {
            conn.getResponseCode();
            break;
          } finally {
            conn.disconnect();
          }
        } catch (IOException ignored) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
      }
      // Phase 2: wait for the bucket-creation route to be registered.
      // Per-attempt probe bucket name avoids stale-state 500s if s3Path is reused across
      // attempts (each attempt's partial directory is left in place after shutdown).
      S3Client probeS3 = createTestS3Client(endpoint);
      String probeBucket = "s3mock-readiness-probe-" + attempt;
      boolean phase2Ok = false;
      for (int bucketAttempt = 0; bucketAttempt < 100; bucketAttempt++) {
        try {
          probeS3.createBucket(CreateBucketRequest.builder().bucket(probeBucket).build());
          phase2Ok = true;
          break;
        } catch (Exception e) {
          lastException = e;
          try {
            Thread.sleep(200);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
      if (phase2Ok) {
        return new StartedMock(mockApi, endpoint);
      }
      // Phase 2 exhausted: shut down the stuck instance, wait for port release, retry.
      mockApi.shutdown();
      for (int i = 0; i < 30; i++) {
        try (java.net.Socket sock = new java.net.Socket("127.0.0.1", port)) {
          Thread.sleep(100);
        } catch (IOException e) {
          break;
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    throw new IOException("Failed to start S3Mock after 5 attempts", lastException);
  }

  public static S3Client createTestS3Client(String endpoint) {
    return S3Client.builder()
        .credentialsProvider(AnonymousCredentialsProvider.create())
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create(endpoint))
        .forcePathStyle(true)
        .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
        .build();
  }

  public static S3AsyncClient createTestS3AsyncClient(String endpoint) {
    return S3AsyncClient.builder()
        .credentialsProvider(AnonymousCredentialsProvider.create())
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create(endpoint))
        .forcePathStyle(true)
        .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
        .build();
  }

  public AmazonS3Provider(String bucketName) {
    this.bucketName = bucketName;
    this.temporaryFolder = new TemporaryFolder();
    this.s3Path = null;
  }

  @Override
  protected void before() throws Throwable {
    temporaryFolder.create();
    s3Path = temporaryFolder.newFolder("s3").toString();
    StartedMock sm = startS3Mock(s3Path);
    api = sm.api();
    s3 = createTestS3Client(sm.endpoint());
    s3Async = createTestS3AsyncClient(sm.endpoint());
    transferManager = S3TransferManager.builder().s3Client(s3Async).build();
    s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
  }

  @Override
  protected void after() {
    if (transferManager != null) {
      transferManager.close();
    }
    if (s3Async != null) {
      s3Async.close();
    }
    if (s3 != null) {
      s3.close();
    }
    if (api != null) {
      api.shutdown();
    }
    temporaryFolder.delete();
  }

  /** Get the test S3 client */
  public S3Client getS3Client() {
    return s3;
  }

  /** Get the test S3 client (deprecated, use getS3Client() instead) */
  @Deprecated
  public S3Client getAmazonS3() {
    return s3;
  }

  /** Get the test S3 async client */
  public S3AsyncClient getS3AsyncClient() {
    return s3Async;
  }

  /** Get the test S3 transfer manager */
  public S3TransferManager getS3TransferManager() {
    return transferManager;
  }

  /** Get the local directory path where mock S3 files are stored */
  public String getS3DirectoryPath() {
    if (s3Path == null) {
      throw new IllegalStateException("S3 not initialized yet");
    }
    return s3Path;
  }
}
