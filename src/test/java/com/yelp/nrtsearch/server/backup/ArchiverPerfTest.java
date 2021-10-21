/*
 * Copyright 2021 Yelp Inc.
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
package com.yelp.nrtsearch.server.backup;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import org.junit.Test;
import scala.util.Random;

public class ArchiverPerfTest {
  private static final String BUCKET_NAME = "yelp-service-data-dev";
  private static final String BOTO_CFG_PATH = "/etc/boto_cfg/elasticlucy.cfg";
  private static final String ARCHIVE_DIR = "/nail/home/umesh/scratch/archiver";
  private static final String SERVICE_NAME = "nrtsearch-lucy-awn";
  private static final String RESOURCE_NAME = "americas_west_north_data";

  @Test
  public void runArchiverPerfDownloadTest() throws IOException, InterruptedException {
    Random rand = new Random();
    ArchiverPerfTest.main(new String[] {String.valueOf(rand.nextString(5))});
  }

  public static void main(String[] args) throws IOException {
    // test download
    AmazonS3 amazonS3 = providesAmazonS3();
    Archiver archiver =
        new ArchiverImpl(
            amazonS3,
            BUCKET_NAME,
            Paths.get(ARCHIVE_DIR, args[0]),
            new TarImpl(Tar.CompressionMode.LZ4),
            true);
    Path downloadPath = archiver.download(SERVICE_NAME, RESOURCE_NAME);
    System.out.println(
        String.format(
            "Downloaded service: %s, resource: %s to path: %s",
            SERVICE_NAME, RESOURCE_NAME, downloadPath.toAbsolutePath()));
  }

  private void multiPartUpload(Archiver archiver) throws IOException {
    archiver.upload(
        "testservice",
        "testresource",
        Paths.get(ARCHIVE_DIR, ""),
        Collections.emptyList(),
        Collections.emptyList(),
        true);
  }

  private static AmazonS3 providesAmazonS3() {
    {
      Path botoCfgPath = Paths.get(BOTO_CFG_PATH);
      final ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile(botoCfgPath.toFile());
      final AWSCredentialsProvider awsCredentialsProvider =
          new ProfileCredentialsProvider(profilesConfigFile, "default");
      AmazonS3 s3ClientInterim =
          AmazonS3ClientBuilder.standard().withCredentials(awsCredentialsProvider).build();
      String region = s3ClientInterim.getBucketLocation(BUCKET_NAME);
      // In useast-1, the region is returned as "US" which is an equivalent to "us-east-1"
      // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/Region.html#US_Standard
      // However, this causes an UnknownHostException so we override it to the full region name
      if (region.equals("US")) {
        region = "us-east-1";
      }
      String serviceEndpoint = String.format("s3.%s.amazonaws.com", region);
      System.out.println(String.format("S3 ServiceEndpoint: %s", serviceEndpoint));
      return AmazonS3ClientBuilder.standard()
          .withCredentials(awsCredentialsProvider)
          .withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
          .build();
    }
  }
}
