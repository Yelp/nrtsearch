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
package com.yelp.nrtsearch.module;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.yelp.nrtsearch.server.concurrent.ExecutorFactory;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.NrtsearchServer;
import com.yelp.nrtsearch.server.modules.BackendModule;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/** A Guice module to initialize {@link NrtsearchServer} instance for tests. */
public class TestNrtsearchModule extends AbstractModule {

  private final NrtsearchConfig nrtsearchConfig;
  private final S3Client amazonS3;
  private final S3AsyncClient amazonS3Async;
  private final ExecutorFactory executorFactory;

  public TestNrtsearchModule(
      NrtsearchConfig nrtsearchConfig,
      S3Client amazonS3,
      S3AsyncClient amazonS3Async,
      ExecutorFactory executorFactory) {
    this.nrtsearchConfig = nrtsearchConfig;
    this.amazonS3 = amazonS3;
    this.amazonS3Async = amazonS3Async;
    this.executorFactory = executorFactory;
  }

  protected void configure() {
    install(new BackendModule());
  }

  @Inject
  @Singleton
  @Provides
  protected NrtsearchConfig providesNrtsearchConfig() {
    return nrtsearchConfig;
  }

  @Inject
  @Singleton
  @Provides
  protected S3Util.S3ClientBundle providesS3ClientBundle() {
    return new S3Util.S3ClientBundle(amazonS3, amazonS3Async);
  }

  @Inject
  @Singleton
  @Provides
  protected ExecutorFactory providesExecutorFactory() {
    return executorFactory;
  }
}
