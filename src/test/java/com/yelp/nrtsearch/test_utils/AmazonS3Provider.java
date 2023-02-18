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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

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
 *      AmazonS3 s3Client = s3Provider.getAmazonS3();
 *      s3Client.putObject("test_bucket", "key", file);
 *      // ...
 *     }
 * }
 * </pre>
 */
public class AmazonS3Provider extends ExternalResource {

  private final String bucketName;
  private final TemporaryFolder temporaryFolder;
  private S3Mock api;
  private AmazonS3 s3;
  private String s3Path;

  public AmazonS3Provider(String bucketName) {
    this.bucketName = bucketName;
    this.temporaryFolder = new TemporaryFolder();
    this.s3Path = null;
  }

  @Override
  protected void before() throws Throwable {
    temporaryFolder.create();
    s3Path = temporaryFolder.newFolder("s3").toString();
    api = S3Mock.create(8011, s3Path);
    api.start();
    s3 =
        AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .withEndpointConfiguration(new EndpointConfiguration("http://127.0.0.1:8011", ""))
            .build();
    s3.createBucket(bucketName);
  }

  @Override
  protected void after() {
    s3.shutdown();
    if (api != null) {
      api.shutdown();
    }
    temporaryFolder.delete();
  }

  /** Get the test S3 client */
  public AmazonS3 getAmazonS3() {
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
