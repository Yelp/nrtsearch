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
package com.yelp.nrtsearch.plugins.example;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yelp.nrtsearch.server.analysis.AnalysisProvider;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.plugins.AbstractIngestionPlugin;
import com.yelp.nrtsearch.server.plugins.AnalysisPlugin;
import com.yelp.nrtsearch.server.plugins.CustomRequestPlugin;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExamplePlugin extends AbstractIngestionPlugin
    implements AnalysisPlugin, CustomRequestPlugin {
  private static final Logger logger = LoggerFactory.getLogger(ExamplePlugin.class);
  public static final String INGESTION_TEST_INDEX = "ingestion_test_index";
  private final String availableAnalyzers = String.join(",", getAnalyzers().keySet());
  private ExecutorService executorService;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final List<AddDocumentRequest> testDocuments = new ArrayList<>();

  public ExamplePlugin(NrtsearchConfig configuration) {
    super(configuration);

    // Create test documents
    testDocuments.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(INGESTION_TEST_INDEX)
            .putFields(
                "field1",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("test doc 1").build())
            .build());
    testDocuments.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(INGESTION_TEST_INDEX)
            .putFields(
                "field1",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("test doc 2").build())
            .build());
  }

  @Override
  protected ExecutorService getIngestionExecutor() {
    return null;
  }

  @Override
  public String id() {
    return "custom_analyzers";
  }

  @Override
  public Map<String, RequestProcessor> getRoutes() {
    return Map.of(
        "get_available_analyzers",
        (path, request) -> Map.of("available_analyzers", availableAnalyzers));
  }

  @Override
  public Map<String, AnalysisProvider<? extends Analyzer>> getAnalyzers() {
    return Map.of(
        "plugin_analyzer",
        (name) -> {
          try {
            return CustomAnalyzer.builder()
                .withDefaultMatchVersion(Version.LATEST)
                .addCharFilter("htmlstrip")
                .withTokenizer("classic")
                .addTokenFilter("lowercase")
                .build();
          } catch (Exception e) {
            return null;
          }
        });
  }

  @Override
  public void startIngestion() throws IOException {
    logger.info("Starting example ingestion");
    if (!running.compareAndSet(false, true)) {
      logger.warn("Ingestion already running");
      return;
    }

    executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("example-ingestion-%d").build());

    executorService.submit(
        () -> {
          try {
            addDocuments(testDocuments, INGESTION_TEST_INDEX);
            commit(INGESTION_TEST_INDEX);
          } catch (Exception e) {
            logger.error("Error during ingestion", e);
          } finally {
            running.set(false); // Reset running flag when done
          }
        });
  }

  @Override
  public void stopIngestion() throws IOException {
    logger.info("Stopping example ingestion");
    running.set(false); // Signal stop (not strictly needed for one-shot task)

    if (executorService != null) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
          logger.warn("Ingestion thread did not complete within timeout");
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while stopping ingestion", e);
      } finally {
        executorService = null; // Allow recreation
      }
    }
  }
}
