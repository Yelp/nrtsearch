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
package com.yelp.nrtsearch.server.luceneserver.state.backend;

import com.yelp.nrtsearch.server.luceneserver.state.PersistentGlobalState;
import java.io.IOException;

/**
 * Interface for a backend managing the loading/storing of {@link PersistentGlobalState}. It can be
 * assumed that external synchronization will prevent interface methods from being called
 * concurrently.
 */
public interface StateBackend {

  /**
   * Load the current state value from the backend. If no state exists, create a new one, persisting
   * it before returning.
   *
   * @return current state value
   * @throws IOException
   */
  PersistentGlobalState loadOrCreateGlobalState() throws IOException;

  /**
   * Commit the given state value to the backend.
   *
   * @param persistentGlobalState state value to commit
   * @throws IOException
   */
  void commitGlobalState(PersistentGlobalState persistentGlobalState) throws IOException;
}
