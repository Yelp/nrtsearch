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

import com.amazonaws.services.s3.AmazonS3;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yelp.nrtsearch.module.TestLuceneServerModule;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.LuceneServer;
import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import java.io.IOException;
import org.junit.rules.ExternalResource;

/**
 * A JUnit {@link org.junit.Rule} that creates a {@link LuceneServer} instance and starts the grpc
 * server from it. After the test it will delete the index data and stop the server.
 */
public class TestLuceneServer extends ExternalResource {

  private final LuceneServerConfiguration luceneServerConfiguration;
  private final AmazonS3 amazonS3;
  private LuceneServer luceneServer;
  private LuceneServerClient client;

  public TestLuceneServer(LuceneServerConfiguration luceneServerConfiguration, AmazonS3 amazonS3) {
    this.luceneServerConfiguration = luceneServerConfiguration;
    this.amazonS3 = amazonS3;
  }

  @Override
  protected void before() throws Throwable {
    this.luceneServer = createTestServer();
    this.client = new LuceneServerClient("localhost", luceneServerConfiguration.getPort());
  }

  @Override
  protected void after() {
    deleteIndexData();
    client.close();
    luceneServer.stop();
  }

  protected LuceneServerClient getClient() {
    return client;
  }

  private LuceneServer createTestServer() throws IOException {
    Injector injector =
        Guice.createInjector(new TestLuceneServerModule(luceneServerConfiguration, amazonS3));
    LuceneServer luceneServer = injector.getInstance(LuceneServer.class);
    luceneServer.start();
    return luceneServer;
  }

  private void deleteIndexData() {
    client.getIndices().forEach(client::deleteIndex);
  }
}
