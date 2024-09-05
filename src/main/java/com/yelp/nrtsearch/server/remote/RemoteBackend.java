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
package com.yelp.nrtsearch.server.remote;

import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;

/** Interface for interacting with service resources stored in a persistent backend. */
public interface RemoteBackend extends PluginDownloader {
  enum IndexResourceType {
    WARMING_QUERIES,
    POINT_STATE,
    INDEX_STATE
  }

  enum GlobalResourceType {
    GLOBAL_STATE
  }

  /**
   * Get if a given global resource exists in the backend.
   *
   * @param service service name
   * @param resourceType type of global resource
   * @return if resource exists
   * @throws IOException on error checking if resource exists
   */
  boolean exists(String service, GlobalResourceType resourceType) throws IOException;

  /**
   * Get if a given index resource exists in the backend.
   *
   * @param service service name
   * @param indexIdentifier unique index identifier
   * @param resourceType type of index resource
   * @return if resource exists
   * @throws IOException on error checking if resource exists
   */
  boolean exists(String service, String indexIdentifier, IndexResourceType resourceType)
      throws IOException;

  /**
   * Upload global state data to the remote backend.
   *
   * @param service service name
   * @param data global state data
   * @throws IOException on error uploading global state
   */
  void uploadGlobalState(String service, byte[] data) throws IOException;

  /**
   * Download global state data from the remote backend.
   *
   * @param service service name
   * @return input stream of global state data
   * @throws IOException on error downloading global state
   */
  InputStream downloadGlobalState(String service) throws IOException;

  /**
   * Upload index state data to the remote backend.
   *
   * @param service service name
   * @param indexIdentifier unique index identifier
   * @param data index state data
   * @throws IOException on error uploading index state
   */
  void uploadIndexState(String service, String indexIdentifier, byte[] data) throws IOException;

  /**
   * Download index state data from the remote backend.
   *
   * @param service service name
   * @param indexIdentifier unique index identifier
   * @return input stream of index state data
   * @throws IOException on error downloading index state
   */
  InputStream downloadIndexState(String service, String indexIdentifier) throws IOException;

  /**
   * Upload warming query data to the remote backend.
   *
   * @param service service name
   * @param indexIdentifier unique index identifier
   * @param data warming query data
   * @throws IOException on error uploading warming queries
   */
  void uploadWarmingQueries(String service, String indexIdentifier, byte[] data) throws IOException;

  /**
   * Download warming query data from the remote backend.
   *
   * @param service service name
   * @param indexIdentifier unique index identifier
   * @return input stream of warming query data
   * @throws IOException on error downloading warming queries
   */
  InputStream downloadWarmingQueries(String service, String indexIdentifier) throws IOException;

  /**
   * Upload index files to the remote backend.
   *
   * @param service service name
   * @param indexIdentifier unique index identifier
   * @param indexDir directory to upload files from
   * @param files map of file names to metadata
   * @throws IOException on error uploading files
   */
  void uploadIndexFiles(
      String service, String indexIdentifier, Path indexDir, Map<String, NrtFileMetaData> files)
      throws IOException;

  /**
   * Download index files from the remote backend.
   *
   * @param service service name
   * @param indexIdentifier unique index identifier
   * @param indexDir directory to download files to
   * @param files map of file names to metadata
   * @throws IOException on error downloading files
   */
  void downloadIndexFiles(
      String service, String indexIdentifier, Path indexDir, Map<String, NrtFileMetaData> files)
      throws IOException;

  /**
   * Upload NRT point state to the remote backend.
   *
   * @param service service name
   * @param indexIdentifier unique index identifier
   * @param nrtPointState NRT point state to upload
   * @throws IOException on error uploading point state
   */
  void uploadPointState(String service, String indexIdentifier, NrtPointState nrtPointState)
      throws IOException;

  /**
   * Download NRT point state from the remote backend.
   *
   * @param service service name
   * @param indexIdentifier unique index identifier
   * @return downloaded NRT point state
   * @throws IOException on error downloading point state
   */
  NrtPointState downloadPointState(String service, String indexIdentifier) throws IOException;
}
