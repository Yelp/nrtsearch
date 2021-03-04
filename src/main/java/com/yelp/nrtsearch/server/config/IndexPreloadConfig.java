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
package com.yelp.nrtsearch.server.config;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Class containing configuration related to index data preloading. */
public class IndexPreloadConfig {
  public static final String ALL_EXTENSIONS = "*";
  public static final IndexPreloadConfig PRELOAD_ALL =
      new IndexPreloadConfig(true, Collections.singleton(ALL_EXTENSIONS));
  private final boolean preload;
  private final Set<String> extensions;

  /**
   * Create instance from provided configuration reader.
   *
   * @param configReader config reader
   * @return class instance
   */
  public static IndexPreloadConfig fromConfig(YamlConfigReader configReader) {
    boolean preload = configReader.getBoolean("preloadIndexData", true);
    List<String> preloadExtensions =
        configReader.getStringList("preloadExtensions", Collections.singletonList(ALL_EXTENSIONS));
    return new IndexPreloadConfig(preload, new HashSet<>(preloadExtensions));
  }

  /**
   * Constructor.
   *
   * @param preload if index data should be preloaded
   * @param extensions set of file extensions that should be preloaded, or * for all
   */
  public IndexPreloadConfig(boolean preload, Set<String> extensions) {
    this.preload = preload;
    this.extensions = Collections.unmodifiableSet(extensions);
  }

  /** Get if any index data should be preloaded. */
  public boolean getShouldPreload() {
    return preload && !extensions.isEmpty();
  }

  /** Get if all index files should be preloaded. */
  public boolean getPreloadAll() {
    return preload && extensions.contains(ALL_EXTENSIONS);
  }

  /** Get file extensions that should be preloaded. */
  public Set<String> getExtensions() {
    return extensions;
  }
}
