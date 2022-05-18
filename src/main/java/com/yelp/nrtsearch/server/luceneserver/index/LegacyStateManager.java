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
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Implementation of state manager that wraps a {@link LegacyIndexState}, for use where a manager is
 * required (like ShardState/ServerCodec). Only supports {@link #getCurrent()} method.
 */
public class LegacyStateManager implements IndexStateManager {
  static final String EXCEPTION_MSG = "Not supported by LEGACY state backend";

  private final IndexState indexState;

  /**
   * Constructor.
   *
   * @param indexState index state to wrap
   */
  public LegacyStateManager(IndexState indexState) {
    this.indexState = indexState;
  }

  @Override
  public void load() throws IOException {
    throw new UnsupportedOperationException(EXCEPTION_MSG);
  }

  @Override
  public void create() throws IOException {
    throw new UnsupportedOperationException(EXCEPTION_MSG);
  }

  @Override
  public IndexSettings getSettings() {
    throw new UnsupportedOperationException(EXCEPTION_MSG);
  }

  @Override
  public IndexSettings updateSettings(IndexSettings settings) throws IOException {
    throw new UnsupportedOperationException(EXCEPTION_MSG);
  }

  @Override
  public IndexLiveSettings getLiveSettings() {
    throw new UnsupportedOperationException(EXCEPTION_MSG);
  }

  @Override
  public IndexLiveSettings updateLiveSettings(IndexLiveSettings liveSettings) throws IOException {
    throw new UnsupportedOperationException(EXCEPTION_MSG);
  }

  @Override
  public String updateFields(List<Field> fields) throws IOException {
    throw new UnsupportedOperationException(EXCEPTION_MSG);
  }

  @Override
  public void start(
      Mode serverMode, Path dataPath, long primaryGen, ReplicationServerClient primaryClient)
      throws IOException {
    throw new UnsupportedOperationException(EXCEPTION_MSG);
  }

  @Override
  public IndexState getCurrent() {
    return indexState;
  }

  @Override
  public void close() throws IOException {}
}
