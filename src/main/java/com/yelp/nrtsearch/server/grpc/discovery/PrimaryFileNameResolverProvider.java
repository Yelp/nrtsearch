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
package com.yelp.nrtsearch.server.grpc.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import java.net.URI;

/** Provides a {@link NameResolver} which can resolve primary address from a json file. */
public class PrimaryFileNameResolverProvider extends NameResolverProvider {

  private final ObjectMapper objectMapper;
  private final int updateInterval;
  private final int portOverride;

  /**
   * Constructor.
   *
   * @param objectMapper jackson object mapper to read discovery file
   * @param updateInterval how often to check for updated hosts
   * @param portOverride if > 0, replaces port value from discovery file
   */
  public PrimaryFileNameResolverProvider(
      ObjectMapper objectMapper, int updateInterval, int portOverride) {
    this.objectMapper = objectMapper;
    this.updateInterval = updateInterval;
    this.portOverride = portOverride;
  }

  @Override
  protected boolean isAvailable() {
    return true;
  }

  @Override
  protected int priority() {
    // Priority is 0-10. Giving slightly higher priority than 5 (considered default).
    return 6;
  }

  @Override
  public String getDefaultScheme() {
    return "file";
  }

  @Override
  public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
    return new PrimaryFileNameResolver(targetUri, objectMapper, updateInterval, portOverride);
  }
}
