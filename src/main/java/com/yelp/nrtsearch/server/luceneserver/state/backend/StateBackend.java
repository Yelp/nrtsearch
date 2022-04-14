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

import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import java.io.IOException;

/**
 * Interface for a backend managing the loading/storing of {@link GlobalStateInfo} and {@link
 * IndexStateInfo}. It can be assumed that external synchronization will prevent interface methods
 * from being called concurrently.
 */
public interface StateBackend {

  /**
   * Load the current state value from the backend. If no state exists, create a new one, persisting
   * it before returning.
   *
   * @return current state value
   * @throws IOException
   */
  GlobalStateInfo loadOrCreateGlobalState() throws IOException;

  /**
   * Commit the given state value to the backend.
   *
   * @param globalStateInfo state value to commit
   * @throws IOException
   */
  void commitGlobalState(GlobalStateInfo globalStateInfo) throws IOException;

  /**
   * Load the current state value for an index from the backend.
   *
   * @param indexIdentifier index id in state backend
   * @return loaded state, or null if not present
   * @throws IOException on error reading state data
   */
  IndexStateInfo loadIndexState(String indexIdentifier) throws IOException;

  /**
   * Commit the given state value for an index to the backend.
   *
   * @param indexIdentifier index id in state backend
   * @param indexStateInfo index state
   * @throws IOException on error writing state data
   */
  void commitIndexState(String indexIdentifier, IndexStateInfo indexStateInfo) throws IOException;
}
