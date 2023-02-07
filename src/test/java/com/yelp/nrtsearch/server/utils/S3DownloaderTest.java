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
package com.yelp.nrtsearch.server.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.amazonaws.services.s3.AmazonS3;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class S3DownloaderTest {
  private static final String BUCKET_NAME = "s3-downloader-test";
  private static final String KEY = "test_key";
  private static final String CONTENT = "test_content";

  @ClassRule public static final AmazonS3Provider S3_PROVIDER = new AmazonS3Provider(BUCKET_NAME);

  private static S3Downloader s3Downloader;

  @BeforeClass
  public static void setup() throws IOException {
    AmazonS3 s3 = S3_PROVIDER.getAmazonS3();
    s3Downloader = new S3Downloader(s3);
    s3.putObject(BUCKET_NAME, KEY, CONTENT);
  }

  @AfterClass
  public static void cleanUp() {
    s3Downloader.close();
  }

  @Test
  public void testDownloadFromS3Path() throws IOException {
    InputStream inputStream = s3Downloader.downloadFromS3Path(BUCKET_NAME, KEY);
    String contentFromS3 = convertToString(inputStream);
    assertEquals(CONTENT, contentFromS3);

    inputStream = s3Downloader.downloadFromS3Path(String.format("s3://%s/%s", BUCKET_NAME, KEY));
    contentFromS3 = convertToString(inputStream);
    assertEquals(CONTENT, contentFromS3);
  }

  @Test
  public void testDownloadFromS3Path_fileDoesNotExist() {
    String s3Path = String.format("s3://%s/%s", BUCKET_NAME, "does_not_exist");
    try {
      s3Downloader.downloadFromS3Path(s3Path);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(String.format("Object %s not found", s3Path), e.getMessage());
    }

    try {
      s3Downloader.downloadFromS3Path("bucket_not_exist", KEY);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          String.format("Object s3://%s/%s not found", "bucket_not_exist", KEY), e.getMessage());
    }
  }

  private String convertToString(InputStream inputStream) throws IOException {
    StringWriter writer = new StringWriter();
    IOUtils.copy(inputStream, writer);
    return writer.toString();
  }
}
