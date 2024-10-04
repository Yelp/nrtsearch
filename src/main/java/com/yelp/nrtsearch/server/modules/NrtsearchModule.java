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
package com.yelp.nrtsearch.server.modules;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.NrtsearchServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Optional;

public class NrtsearchModule extends AbstractModule {
  private static final String DEFAULT_CONFIG_FILE_RESOURCE =
      "/lucene_server_default_configuration.yaml";
  private final NrtsearchServer.NrtsearchServerCommand args;

  public NrtsearchModule(NrtsearchServer.NrtsearchServerCommand args) {
    this.args = args;
  }

  @Override
  protected void configure() {
    install(new S3Module());
    install(new BackendModule());
  }

  @Inject
  @Singleton
  @Provides
  public PrometheusRegistry providesPrometheusRegistry() {
    return new PrometheusRegistry();
  }

  @Inject
  @Singleton
  @Provides
  protected NrtsearchConfig providesNrtsearchConfig() throws FileNotFoundException {
    NrtsearchConfig luceneServerConfiguration;
    Optional<File> maybeConfigFile = args.maybeConfigFile();
    if (maybeConfigFile.isEmpty()) {
      luceneServerConfiguration =
          new NrtsearchConfig(getClass().getResourceAsStream(DEFAULT_CONFIG_FILE_RESOURCE));
    } else {
      luceneServerConfiguration = new NrtsearchConfig(new FileInputStream(maybeConfigFile.get()));
    }
    return luceneServerConfiguration;
  }
}
