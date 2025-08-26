/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.ingestion;

import com.yelp.nrtsearch.server.plugins.IngestionPlugin;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestionPluginUtils {
  private static final Logger logger = LoggerFactory.getLogger(IngestionPluginUtils.class);
  public static void initializeAndStart(IngestionPlugin plugin, GlobalState globalState)
      throws IOException {
    Ingestor ingestor = plugin.getIngestor();
    if (ingestor instanceof AbstractIngestor abstractIngestor) {
      abstractIngestor.initialize(globalState);
    }

    ExecutorService executor = plugin.getIngestionExecutor();
    executor.submit(
        () -> {
          try {
            ingestor.start();
          } catch (IOException e) {
            logger.error("Failed to start ingestor", e);
          }
        });
  }
}
