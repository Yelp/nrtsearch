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

/** Configuration for selective page cache eviction after index file writes. */
public class PageCacheEvictionConfig {
  private static final String CONFIG_PREFIX = "evictOnWrite.";
  public static final String ALL_EXTENSIONS = "*";

  private final boolean enabled;
  private final Set<String> extensions;

  /**
   * Create instance from provided configuration reader.
   *
   * @param configReader config reader
   * @return class instance
   */
  public static PageCacheEvictionConfig fromConfig(YamlConfigReader configReader) {
    boolean enabled = configReader.getBoolean(CONFIG_PREFIX + "enabled", false);
    List<String> extensionList =
        configReader.getStringList(CONFIG_PREFIX + "extensions", Collections.emptyList());
    return new PageCacheEvictionConfig(enabled, new HashSet<>(extensionList));
  }

  /**
   * Constructor.
   *
   * @param enabled if page cache eviction is enabled
   * @param extensions set of file extensions to evict, or * for all extensions
   */
  public PageCacheEvictionConfig(boolean enabled, Set<String> extensions) {
    this.enabled = enabled;
    this.extensions = Collections.unmodifiableSet(extensions);
  }

  /**
   * Returns true if the given file extension should have its pages evicted after writing.
   *
   * @param extension file extension (without leading dot)
   * @return true if pages should be evicted
   */
  public boolean shouldEvict(String extension) {
    return enabled && (extensions.contains(ALL_EXTENSIONS) || extensions.contains(extension));
  }

  public boolean isEnabled() {
    return enabled;
  }

  @VisibleForTesting
  Set<String> getExtensions() {
    return extensions;
  }
}
