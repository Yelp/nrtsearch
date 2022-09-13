/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.index;

import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Interface for operation related to managing index state for a single index. Used for
 * getting/mutating state (settings, live settings, fields), as well as operations requiring
 * exclusive control of state (like starting the index).
 */
public interface IndexStateManager extends Closeable {

  /**
   * Load the currently committed index state. This state is expected to exist and a {@link
   * RuntimeException} should be thrown if that is not the case.
   *
   * @throws IOException on error reading state
   */
  void load() throws IOException;

  /**
   * Initialize and commit new index state. If state for this index has already been committed, a
   * {@link RuntimeException} should be thrown.
   *
   * @throws IOException on error accessing state
   */
  void create() throws IOException;

  /** Get the current index settings. */
  IndexSettings getSettings();

  /**
   * Update the index setting from the given input settings. Input settings will be merged into the
   * existing index settings, replacing any values that exist. These settings can only be updated if
   * the index is stopped.
   *
   * @param settings settings modifications
   * @return merged index settings, including default values
   * @throws IOException on error accessing state
   */
  IndexSettings updateSettings(IndexSettings settings) throws IOException;

  /** Get the current index live settings. */
  IndexLiveSettings getLiveSettings();

  /**
   * Update the index live setting from the given input settings. Input settings will be merged into
   * the existing index settings, replacing any values that exist.
   *
   * @param liveSettings live settings modifications
   * @return merged index settings, including default values
   * @throws IOException on error accessing state
   */
  IndexLiveSettings updateLiveSettings(IndexLiveSettings liveSettings) throws IOException;

  /**
   * Update index fields with the given {@link Field} messages. Current only supports addition of
   * new fields.
   *
   * @param fields field updates
   * @return Json string of current index fields map
   * @throws IOException on error accessing state
   */
  String updateFields(List<Field> fields) throws IOException;

  /**
   * Start the index.
   *
   * @param serverMode index mode
   * @param dataPath path to any restored index data, or null
   * @param primaryGen primary generation number
   * @param primaryClient replication client for replicas to talk to primary
   * @throws IOException on error accessing index data
   */
  void start(Mode serverMode, Path dataPath, long primaryGen, ReplicationServerClient primaryClient)
      throws IOException;

  /**
   * Get the current index state. This instance may be immutable, requiring additional retrievals to
   * see changes.
   *
   * @return current index state
   */
  IndexState getCurrent();

  /**
   * Get the index id
   *
   * @return
   */
  String getIndexId();
}
