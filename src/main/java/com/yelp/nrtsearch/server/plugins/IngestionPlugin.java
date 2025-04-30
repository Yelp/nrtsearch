/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.plugins;

import java.io.IOException;

/** Interface for ingestion plugins. Defines lifecycle methods that plugins must implement. */
public interface IngestionPlugin {

  /**
   * Start ingesting documents from the source. Plugin implementations can start source connections
   * and begin processing.
   *
   * @throws IOException if there are startup errors
   */
  void startIngestion() throws IOException;

  /**
   * Stop ingesting documents from the source. Plugin implementations should cleanup source
   * connections and stop processing.
   *
   * @throws IOException if there are shutdown errors
   */
  void stopIngestion() throws IOException;
}
