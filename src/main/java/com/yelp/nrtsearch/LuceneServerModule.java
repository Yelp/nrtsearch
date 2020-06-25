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
package com.yelp.nrtsearch;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.utils.Archiver;
import com.yelp.nrtsearch.server.utils.ArchiverImpl;
import com.yelp.nrtsearch.server.utils.Tar;
import com.yelp.nrtsearch.server.utils.TarImpl;
import io.prometheus.client.CollectorRegistry;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LuceneServerModule extends AbstractModule {
  private final String[] args;

  public LuceneServerModule(String[] args) {
    this.args = args;
  }

  @Inject
  @Singleton
  @Provides
  public Tar providesTar() {
    return new TarImpl(Tar.CompressionMode.LZ4);
  }

  @Inject
  @Singleton
  @Provides
  public CollectorRegistry providesCollectorRegistry() {
    return new CollectorRegistry();
  }

  @Inject
  @Singleton
  @Provides
  protected AmazonS3 providesAmazonS3(LuceneServerConfiguration luceneServerConfiguration) {
    if (luceneServerConfiguration
        .getBotoCfgPath()
        .equals(LuceneServerConfiguration.DEFAULT_BOTO_CFG_PATH.toString())) {
      return AmazonS3ClientBuilder.standard()
          .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
          .withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration("dummyService", "dummyRegion"))
          .build();
    } else {
      Path botoCfgPath = Paths.get(luceneServerConfiguration.getBotoCfgPath());
      final ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile(botoCfgPath.toFile());
      final AWSCredentialsProvider awsCredentialsProvider =
          new ProfileCredentialsProvider(profilesConfigFile, "default");
      AmazonS3 s3ClientInterim =
          AmazonS3ClientBuilder.standard().withCredentials(awsCredentialsProvider).build();
      String region = s3ClientInterim.getBucketLocation(luceneServerConfiguration.getBucketName());
      String serviceEndpoint = String.format("s3.%s.amazonaws.com", region);
      return AmazonS3ClientBuilder.standard()
          .withCredentials(awsCredentialsProvider)
          .withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
          .build();
    }
  }

  @Inject
  @Singleton
  @Provides
  protected Archiver providesArchiver(
      LuceneServerConfiguration luceneServerConfiguration, AmazonS3 amazonS3, Tar tar) {
    Path archiveDir = Paths.get(luceneServerConfiguration.getArchiveDirectory());
    return new ArchiverImpl(amazonS3, luceneServerConfiguration.getBucketName(), archiveDir, tar);
  }

  @Inject
  @Singleton
  @Provides
  protected LuceneServerConfiguration providesLuceneServerConfiguration()
      throws FileNotFoundException {
    LuceneServerConfiguration luceneServerConfiguration;
    if (args.length == 0) {
      Path filePath =
          Paths.get("src", "main", "resources", "lucene_server_default_configuration.yaml");
      luceneServerConfiguration =
          new LuceneServerConfiguration(new FileInputStream(filePath.toFile()));
    } else {
      luceneServerConfiguration = new LuceneServerConfiguration(new FileInputStream(args[0]));
    }
    return luceneServerConfiguration;
  }
}
