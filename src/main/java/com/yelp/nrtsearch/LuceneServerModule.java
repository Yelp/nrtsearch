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

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.LuceneServer;
import com.yelp.nrtsearch.server.module.S3Module;
import io.prometheus.client.CollectorRegistry;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Optional;

public class LuceneServerModule extends AbstractModule {
  private static final String DEFAULT_CONFIG_FILE_RESOURCE =
      "/lucene_server_default_configuration.yaml";
  private final LuceneServer.LuceneServerCommand args;

  public LuceneServerModule(LuceneServer.LuceneServerCommand args) {
    this.args = args;
  }

  @Override
  protected void configure() {
    install(new S3Module());
    install(new ArchiverModule());
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
  protected LuceneServerConfiguration providesLuceneServerConfiguration()
      throws FileNotFoundException {
    LuceneServerConfiguration luceneServerConfiguration;
    Optional<File> maybeConfigFile = args.maybeConfigFile();
    if (maybeConfigFile.isEmpty()) {
      luceneServerConfiguration =
          new LuceneServerConfiguration(
              getClass().getResourceAsStream(DEFAULT_CONFIG_FILE_RESOURCE));
    } else {
      luceneServerConfiguration =
          new LuceneServerConfiguration(new FileInputStream(maybeConfigFile.get()));
    }
    return luceneServerConfiguration;
  }
}
