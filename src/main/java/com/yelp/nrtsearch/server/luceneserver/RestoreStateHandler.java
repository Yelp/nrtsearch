/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.utils.Archiver;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads metadata/state for each resource/index that has been backed-up on s3 and loads those
 * indexNames in GlobalState (as a JSONObject) Once the restore method is called, any calls to
 * globalState.getIndex(indexName) on the backed-up index names should return the appropriate
 * IndexState object.
 */
public class RestoreStateHandler {
  private static final Logger logger = LoggerFactory.getLogger(RestoreStateHandler.class);

  /**
   * @param serviceName Name of service that identifies this cluster/deployment
   * @param archiver The singleton Archiver object used for this application
   * @param globalState The singleton GlobalState object used for this application
   * @return List of index names that were previously backed-up or an empty List if none were for
   *     the specified serviceName
   * @throws IOException
   * @see Archiver
   * @see GlobalState
   */
  public static List<String> restore(Archiver archiver, GlobalState globalState, String serviceName)
      throws IOException {
    List<String> indexNames = new ArrayList<>();
    List<String> resources = archiver.getResources(serviceName);
    for (String resource : resources) {
      if (resource.contains("_metadata")) {
        Path path = archiver.download(serviceName, resource);
        logger.info(
            String.format(
                "Downloaded state dir at: %s for service: %s, resource: %s",
                path.toString(), serviceName, resource));
        globalState.setStateDir(path);
        indexNames.add(resource);
      }
    }
    return indexNames;
  }
}
