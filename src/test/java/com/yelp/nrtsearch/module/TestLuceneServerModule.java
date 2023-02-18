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

import com.amazonaws.services.s3.AmazonS3;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.yelp.nrtsearch.ArchiverModule;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;

/**
 * A Guice module to initialize {@link com.yelp.nrtsearch.server.grpc.LuceneServer} instance for
 * tests.
 */
public class TestLuceneServerModule extends AbstractModule {

  private final LuceneServerConfiguration luceneServerConfiguration;
  private final AmazonS3 amazonS3;

  public TestLuceneServerModule(
      LuceneServerConfiguration luceneServerConfiguration, AmazonS3 amazonS3) {
    this.luceneServerConfiguration = luceneServerConfiguration;
    this.amazonS3 = amazonS3;
  }

  protected void configure() {
    install(new ArchiverModule());
  }

  @Inject
  @Singleton
  @Provides
  protected LuceneServerConfiguration providesLuceneServerConfiguration() {
    return luceneServerConfiguration;
  }

  @Inject
  @Singleton
  @Provides
  protected AmazonS3 providesAmazonS3() {
    return amazonS3;
  }
}
