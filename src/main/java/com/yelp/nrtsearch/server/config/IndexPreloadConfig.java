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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;

/** Class containing configuration related to index data preloading. */
public class IndexPreloadConfig {
  private static final String CONFIG_PREFIX = "preload.";
  public static final String ALL_EXTENSIONS = "*";
  private final boolean preload;
  private final Set<String> extensions;

  /**
   * Create instance from provided configuration reader.
   *
   * @param configReader config reader
   * @return class instance
   */
  public static IndexPreloadConfig fromConfig(YamlConfigReader configReader) {
    boolean preload = configReader.getBoolean(CONFIG_PREFIX + "enabled", false);
    List<String> preloadExtensions =
        configReader.getStringList(
            CONFIG_PREFIX + "extensions", Collections.singletonList(ALL_EXTENSIONS));
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

  /**
   * Get predicate to determine if an {@link MMapDirectory} file should be preloaded.
   *
   * @return predicate to determine if a file should be preloaded
   */
  public BiPredicate<String, IOContext> preloadPredicate() {
    if (preload) {
      if (extensions.contains(ALL_EXTENSIONS)) {
        return MMapDirectory.ALL_FILES;
      }
      return (fileName, context) -> {
        String extension = FileSwitchDirectory.getExtension(fileName);
        return extensions.contains(extension);
      };
    }
    return MMapDirectory.NO_FILES;
  }

  @VisibleForTesting
  boolean getPreload() {
    return preload;
  }

  @VisibleForTesting
  Set<String> getExtensions() {
    return extensions;
  }
}
