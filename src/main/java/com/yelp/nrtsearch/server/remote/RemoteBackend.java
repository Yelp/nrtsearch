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
    POINT_STATE
  }

  /**
   * Get if a given index resource exists in the backend.
   *
   * @param service service name
   * @param indexIdentifier unique index identifier
   * @param resourceType type of index resource
   * @return if resource exists
   * @throws IOException
   */
  boolean exists(String service, String indexIdentifier, IndexResourceType resourceType)
      throws IOException;

  /**
   * Download index resource data from backend through an {@link InputStream}.
   *
   * @param service service name
   * @param indexIdentifier unique index identifier
   * @param resourceType type of index resource
   * @return input stream to process downloaded resource
   * @throws IllegalArgumentException if resource does not exist
   * @throws IOException
   */
  InputStream downloadStream(String service, String indexIdentifier, IndexResourceType resourceType)
      throws IOException;

  /**
   * Upload file contents to the specified index resource, replacing any existing version.
   *
   * @param service service name
   * @param indexIdentifier unique index identifier
   * @param resourceType type of index resource
   * @param file file data to upload
   * @throws IllegalArgumentException if file does not exist, or is not a regular file
   * @throws IOException on error uploading file
   */
  void uploadFile(String service, String indexIdentifier, IndexResourceType resourceType, Path file)
      throws IOException;

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
