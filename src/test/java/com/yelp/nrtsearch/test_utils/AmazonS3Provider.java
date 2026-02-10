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
import java.net.URI;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

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

  private final String bucketName;
  private final TemporaryFolder temporaryFolder;
  private S3Mock api;
  private S3Client s3;
  private String s3Path;

  public static S3Client createTestS3Client(String endpoint) {
    return S3Client.builder()
        .credentialsProvider(AnonymousCredentialsProvider.create())
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create(endpoint))
        .forcePathStyle(true)
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
    int port = PortUtils.findAvailablePort();
    api = new S3Mock.Builder().withPort(port).withFileBackend(s3Path).build();
    api.start();
    s3 = createTestS3Client(String.format("http://127.0.0.1:%d", port));
    s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
  }

  @Override
  protected void after() {
    s3.close();
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

  /** Get the local directory path where mock S3 files are stored */
  public String getS3DirectoryPath() {
    if (s3Path == null) {
      throw new IllegalStateException("S3 not initialized yet");
    }
    return s3Path;
  }
}
