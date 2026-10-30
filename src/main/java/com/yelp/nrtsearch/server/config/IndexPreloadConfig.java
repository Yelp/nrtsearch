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
import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.Action;
import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.IndexPageCachePolicy;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;

/**
 * Class containing configuration related to index data preloading. The preload decision is made by
 * an {@link IndexPageCachePolicy}: files with the {@link Action#PRELOAD} action are preloaded.
 *
 * <p>The {@code preload.*} config is deprecated in favor of {@link PageCachePolicyConfig} rules.
 */
public class IndexPreloadConfig {
  private static final String CONFIG_PREFIX = "preload.";
  public static final String ALL_EXTENSIONS = "*";
  private static final String COMPOUND_DATA_EXTENSION = "cfs";
  private final boolean preload;
  private final Set<String> extensions;
  private final IndexPageCachePolicy policy;
  private final boolean compoundFileRanges;

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
    this.policy =
        new PageCachePolicyConfig(
                PageCachePolicyConfig.fromLegacyPreload(preload, extensions),
                Collections.emptyMap())
            .getGlobalPolicy();
    this.compoundFileRanges = false;
  }

  /**
   * Create instance that preloads files based on an index page cache policy.
   *
   * @param policy index page cache policy
   */
  public IndexPreloadConfig(IndexPageCachePolicy policy) {
    this(policy, false);
  }

  /**
   * Create instance that preloads files based on an index page cache policy.
   *
   * @param policy index page cache policy
   * @param compoundFileRanges if compound files should be excluded from the preload predicate,
   *     because their sub-file ranges are preloaded separately
   */
  public IndexPreloadConfig(IndexPageCachePolicy policy, boolean compoundFileRanges) {
    this.preload = policy.hasAction(Action.PRELOAD);
    this.extensions = Collections.emptySet();
    this.policy = policy;
    this.compoundFileRanges = compoundFileRanges;
  }

  /**
   * Get a copy of this config that excludes compound files from the preload predicate, because
   * their sub-file ranges are preloaded separately.
   */
  public IndexPreloadConfig withCompoundFileRanges() {
    return new IndexPreloadConfig(policy, true);
  }

  /**
   * Get predicate to determine if an {@link MMapDirectory} file should be preloaded.
   *
   * @return predicate to determine if a file should be preloaded
   */
  public BiPredicate<String, IOContext> preloadPredicate() {
    if (!preload) {
      return MMapDirectory.NO_FILES;
    }
    if (compoundFileRanges) {
      return (fileName, context) ->
          !COMPOUND_DATA_EXTENSION.equals(IndexFileNames.getExtension(fileName))
              && policy.indexFileAction(fileName) == Action.PRELOAD;
    }
    if (policy.preloadsAllFiles()) {
      return MMapDirectory.ALL_FILES;
    }
    return (fileName, context) -> policy.indexFileAction(fileName) == Action.PRELOAD;
  }

  /** Get the page cache policy used to decide which files are preloaded. */
  public IndexPageCachePolicy getPolicy() {
    return policy;
  }

  /** Get if any files may be preloaded. */
  @VisibleForTesting
  boolean getPreload() {
    return preload;
  }

  /** Get the extensions from the deprecated preload config, empty when created from a policy. */
  @VisibleForTesting
  Set<String> getExtensions() {
    return extensions;
  }
}
