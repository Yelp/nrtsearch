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
package com.yelp.nrtsearch.server.remote;

import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import com.yelp.nrtsearch.server.utils.JsonUtils;
import java.io.IOException;

public class RemoteUtils {

  private RemoteUtils() {}

  /**
   * Convert a {@link NrtPointState} to a UTF-8 encoded byte array.
   *
   * @param pointState point state
   * @return UTF-8 encoded byte array
   * @throws IOException on error converting to UTF-8
   */
  public static byte[] pointStateToUtf8(NrtPointState pointState) throws IOException {
    String jsonString = JsonUtils.objectToJsonStr(pointState);
    return StateUtils.toUTF8(jsonString);
  }

  /**
   * Convert a UTF-8 encoded byte array to a {@link NrtPointState}.
   *
   * @param data UTF-8 encoded byte array
   * @return NrtPointState
   * @throws IOException on error converting from UTF-8
   */
  public static NrtPointState pointStateFromUtf8(byte[] data) throws IOException {
    String jsonString = StateUtils.fromUTF8(data);
    return JsonUtils.readValue(jsonString, NrtPointState.class);
  }
}
