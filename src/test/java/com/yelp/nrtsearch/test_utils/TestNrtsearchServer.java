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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yelp.nrtsearch.module.TestNrtsearchModule;
import com.yelp.nrtsearch.server.concurrent.ExecutorFactory;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import com.yelp.nrtsearch.server.grpc.NrtsearchServer;
import java.io.IOException;
import org.junit.rules.ExternalResource;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * A JUnit {@link org.junit.Rule} that creates a {@link NrtsearchServer} instance and starts the
 * grpc server from it. After the test it will delete the index data and stop the server.
 */
public class TestNrtsearchServer extends ExternalResource {

  private final NrtsearchConfig configuration;
  private final S3Client amazonS3;
  private final ExecutorFactory executorFactory;
  private NrtsearchServer server;
  private NrtsearchClient client;

  public TestNrtsearchServer(NrtsearchConfig configuration, S3Client amazonS3) {
    this.configuration = configuration;
    this.amazonS3 = amazonS3;
    this.executorFactory = new ExecutorFactory(configuration.getThreadPoolConfiguration());
  }

  @Override
  protected void before() throws Throwable {
    this.server = createTestServer();
    this.client = new NrtsearchClient("localhost", configuration.getPort());
  }

  @Override
  protected void after() {
    deleteIndexData();
    client.close();
    server.stop();
    try {
      executorFactory.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected NrtsearchClient getClient() {
    return client;
  }

  private NrtsearchServer createTestServer() throws IOException {
    Injector injector =
        Guice.createInjector(new TestNrtsearchModule(configuration, amazonS3, executorFactory));
    NrtsearchServer server = injector.getInstance(NrtsearchServer.class);
    server.start();
    return server;
  }

  private void deleteIndexData() {
    client.getIndices().forEach(client::deleteIndex);
  }
}
