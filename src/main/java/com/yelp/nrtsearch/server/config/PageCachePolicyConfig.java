/*
 * Copyright 2026 Yelp Inc.
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

import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;
import org.apache.lucene.index.IndexFileNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration of the page cache policy for index files. The policy is an ordered list of rules
 * that map index file names to an {@link Action}. Rules may be set globally, and for individual
 * indices. Index rules are checked before global rules, and the first matching rule wins. Files
 * that match no rule use {@link Action#DEFAULT}.
 *
 * <p>Rule globs are matched against segment file names with the segment name prefix removed (see
 * {@link IndexFileNames#stripSegmentName(String)}). For example, {@code _0_Lucene104_0.tip} is
 * matched as {@code _Lucene104_0.tip}, and {@code _0.fdt} as {@code .fdt}. This is the same form
 * used for the sub-file names in compound files, so the same glob matches a standalone file and a
 * compound sub-file. Other files, such as {@code segments_N}, are matched by their full name.
 *
 * <pre>
 * pageCache:
 *   rules:
 *     - files: ["*QuantizedVectorsFormat_*.vec"]
 *       action: EVICT
 *     - files: ["*"]
 *       action: PRELOAD
 *   indices:
 *     my_index:
 *       rules:
 *         - files: ["*.fdt"]
 *           action: EVICT
 * </pre>
 *
 * <p>If the {@code pageCache} section is not set, rules are created from the deprecated {@code
 * preload.*} settings. Setting both is an error.
 */
public class PageCachePolicyConfig {
  private static final Logger logger = LoggerFactory.getLogger(PageCachePolicyConfig.class);

  static final String CONFIG_KEY = "pageCache";
  static final String LEGACY_PRELOAD_KEY = "preload";
  private static final String RULES_KEY = "rules";
  private static final String INDICES_KEY = "indices";
  private static final String FILES_KEY = "files";
  private static final String ACTION_KEY = "action";

  /** Page cache action for an index file range. */
  public enum Action {
    /** Make the data resident in the page cache when it is opened. */
    PRELOAD,
    /** Drop the data from the page cache after it is copied to a replica. */
    EVICT,
    /** Leave the data to the OS page cache. */
    DEFAULT
  }

  /**
   * Rule that maps index files matching any of the given globs to an action.
   *
   * @param files globs to match against file names with the segment name prefix removed
   * @param action action for matching files
   */
  public record Rule(List<String> files, Action action) {
    public Rule {
      if (files == null || files.isEmpty()) {
        throw new IllegalArgumentException("Page cache rule must have at least one file glob");
      }
      if (action == null) {
        throw new IllegalArgumentException("Page cache rule must have an action");
      }
      files = List.copyOf(files);
    }
  }

  private final List<Rule> globalRules;
  private final Map<String, List<Rule>> indexRules;
  private final IndexPageCachePolicy globalPolicy;

  /**
   * Create instance from provided configuration reader.
   *
   * @param configReader config reader
   * @return class instance
   * @throws IllegalArgumentException if the config is invalid, or if both {@code pageCache} and the
   *     deprecated {@code preload} settings are set
   */
  public static PageCachePolicyConfig fromConfig(YamlConfigReader configReader) {
    Object pageCacheConfig = configReader.get(CONFIG_KEY, obj -> obj, null);
    boolean hasLegacyPreload = configReader.get(LEGACY_PRELOAD_KEY, obj -> obj, null) != null;
    if (pageCacheConfig != null) {
      if (hasLegacyPreload) {
        throw new IllegalArgumentException(
            "Only one of "
                + CONFIG_KEY
                + " and "
                + LEGACY_PRELOAD_KEY
                + " may be set, "
                + LEGACY_PRELOAD_KEY
                + " is deprecated");
      }
      return parse(pageCacheConfig);
    }
    if (hasLegacyPreload) {
      logger.warn(
          "The {} config is deprecated, use {} rules instead", LEGACY_PRELOAD_KEY, CONFIG_KEY);
    }
    IndexPreloadConfig legacyConfig = IndexPreloadConfig.fromConfig(configReader);
    return new PageCachePolicyConfig(
        fromLegacyPreload(legacyConfig.getPreload(), legacyConfig.getExtensions()), Map.of());
  }

  /**
   * Create rules equivalent to the deprecated {@code preload.*} settings.
   *
   * @param preload if index data should be preloaded
   * @param extensions file extensions to preload, or {@link IndexPreloadConfig#ALL_EXTENSIONS}
   * @return equivalent rules
   */
  static List<Rule> fromLegacyPreload(boolean preload, Set<String> extensions) {
    if (!preload || extensions.isEmpty()) {
      return List.of();
    }
    if (extensions.contains(IndexPreloadConfig.ALL_EXTENSIONS)) {
      return List.of(new Rule(List.of("*"), Action.PRELOAD));
    }
    List<String> globs =
        extensions.stream().sorted().map(extension -> "*." + escapeGlob(extension)).toList();
    return List.of(new Rule(globs, Action.PRELOAD));
  }

  /**
   * Constructor.
   *
   * @param globalRules rules for all indices
   * @param indexRules rules for specific indices, checked before the global rules
   */
  public PageCachePolicyConfig(List<Rule> globalRules, Map<String, List<Rule>> indexRules) {
    this.globalRules = List.copyOf(globalRules);
    Map<String, List<Rule>> indexRulesCopy = new HashMap<>();
    indexRules.forEach((index, rules) -> indexRulesCopy.put(index, List.copyOf(rules)));
    this.indexRules = Collections.unmodifiableMap(indexRulesCopy);
    this.globalPolicy = new IndexPageCachePolicy(this.globalRules);
  }

  /** Get rules for all indices. */
  public List<Rule> getGlobalRules() {
    return globalRules;
  }

  /** Get rules for specific indices. */
  public Map<String, List<Rule>> getIndexRules() {
    return indexRules;
  }

  /**
   * Get the policy for an index. This contains the index rules, followed by the global rules.
   *
   * @param indexName index name
   * @return index policy
   */
  public IndexPageCachePolicy forIndex(String indexName) {
    List<Rule> rules = indexRules.get(indexName);
    if (rules == null || rules.isEmpty()) {
      return globalPolicy;
    }
    List<Rule> combined = new ArrayList<>(rules.size() + globalRules.size());
    combined.addAll(rules);
    combined.addAll(globalRules);
    return new IndexPageCachePolicy(combined);
  }

  /** Get the policy for indices that have no index rules. */
  public IndexPageCachePolicy getGlobalPolicy() {
    return globalPolicy;
  }

  private static PageCachePolicyConfig parse(Object config) {
    Map<String, Object> configMap = asMap(config, CONFIG_KEY);
    checkKeys(configMap, CONFIG_KEY, RULES_KEY, INDICES_KEY);
    List<Rule> globalRules = parseRules(configMap.get(RULES_KEY), CONFIG_KEY + "." + RULES_KEY);

    Map<String, List<Rule>> indexRules = new HashMap<>();
    Object indices = configMap.get(INDICES_KEY);
    if (indices != null) {
      String indicesPath = CONFIG_KEY + "." + INDICES_KEY;
      for (Map.Entry<String, Object> entry : asMap(indices, indicesPath).entrySet()) {
        String indexPath = indicesPath + "." + entry.getKey();
        Map<String, Object> indexConfig = asMap(entry.getValue(), indexPath);
        checkKeys(indexConfig, indexPath, RULES_KEY);
        indexRules.put(
            entry.getKey(), parseRules(indexConfig.get(RULES_KEY), indexPath + "." + RULES_KEY));
      }
    }
    return new PageCachePolicyConfig(globalRules, indexRules);
  }

  private static List<Rule> parseRules(Object rules, String path) {
    if (rules == null) {
      return List.of();
    }
    if (!(rules instanceof List<?> rulesList)) {
      throw new IllegalArgumentException(path + " must be a list");
    }
    List<Rule> parsed = new ArrayList<>();
    for (int i = 0; i < rulesList.size(); ++i) {
      parsed.add(parseRule(rulesList.get(i), path + "[" + i + "]"));
    }
    return parsed;
  }

  private static Rule parseRule(Object rule, String path) {
    Map<String, Object> ruleMap = asMap(rule, path);
    checkKeys(ruleMap, path, FILES_KEY, ACTION_KEY);

    Object files = ruleMap.get(FILES_KEY);
    if (!(files instanceof List<?> filesList) || filesList.isEmpty()) {
      throw new IllegalArgumentException(path + "." + FILES_KEY + " must be a non-empty list");
    }
    List<String> globs = new ArrayList<>();
    for (Object file : filesList) {
      if (!(file instanceof String glob) || glob.isEmpty()) {
        throw new IllegalArgumentException(
            path + "." + FILES_KEY + " must only contain non-empty strings");
      }
      try {
        IndexPageCachePolicy.createMatcher(glob);
      } catch (PatternSyntaxException | InvalidPathException e) {
        throw new IllegalArgumentException(
            "Invalid glob in " + path + "." + FILES_KEY + ": " + glob, e);
      }
      globs.add(glob);
    }

    Object action = ruleMap.get(ACTION_KEY);
    if (!(action instanceof String actionString)) {
      throw new IllegalArgumentException(path + "." + ACTION_KEY + " must be set");
    }
    try {
      return new Rule(globs, Action.valueOf(actionString.toUpperCase(Locale.ROOT)));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid "
              + path
              + "."
              + ACTION_KEY
              + ": "
              + actionString
              + ", must be one of "
              + Arrays.toString(Action.values()));
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asMap(Object obj, String path) {
    if (!(obj instanceof Map)) {
      throw new IllegalArgumentException(path + " must be a map");
    }
    return (Map<String, Object>) obj;
  }

  private static void checkKeys(Map<String, Object> map, String path, String... allowedKeys) {
    Set<String> allowed = Set.of(allowedKeys);
    for (String key : map.keySet()) {
      if (!allowed.contains(key)) {
        throw new IllegalArgumentException(
            "Unknown key in " + path + ": " + key + ", allowed keys: " + allowed);
      }
    }
  }

  private static String escapeGlob(String value) {
    StringBuilder sb = new StringBuilder();
    for (char c : value.toCharArray()) {
      if ("\\*?[]{}".indexOf(c) >= 0) {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /** Page cache policy for a single index. */
  public static class IndexPageCachePolicy {
    private final List<Rule> rules;
    private final List<List<PathMatcher>> matchers;

    IndexPageCachePolicy(List<Rule> rules) {
      this.rules = List.copyOf(rules);
      this.matchers =
          this.rules.stream()
              .map(rule -> rule.files().stream().map(IndexPageCachePolicy::createMatcher).toList())
              .toList();
    }

    static PathMatcher createMatcher(String glob) {
      return FileSystems.getDefault().getPathMatcher("glob:" + glob);
    }

    /** Get the rules in this policy, in the order they are checked. */
    public List<Rule> getRules() {
      return rules;
    }

    /** Get if all files are preloaded, because the first rule is a catch all {@code PRELOAD}. */
    public boolean preloadsAllFiles() {
      return !rules.isEmpty()
          && rules.getFirst().action() == Action.PRELOAD
          && rules.getFirst().files().contains("*");
    }

    /** Get if any rule in this policy has the given action. */
    public boolean hasAction(Action action) {
      for (Rule rule : rules) {
        if (rule.action() == action) {
          return true;
        }
      }
      return false;
    }

    /**
     * Get the action for an index file.
     *
     * @param fileName index file name
     * @return action from the first matching rule, or {@link Action#DEFAULT}
     */
    public Action indexFileAction(String fileName) {
      return fileAction(stripSegmentName(fileName));
    }

    /**
     * Remove the segment name prefix from a segment file name. Names that are not segment files,
     * which start with an underscore, are returned unchanged.
     */
    static String stripSegmentName(String fileName) {
      if (!fileName.startsWith("_")) {
        return fileName;
      }
      return IndexFileNames.stripSegmentName(fileName);
    }

    /**
     * Get the action for a file name with the segment name prefix removed, such as a compound file
     * sub-file name.
     *
     * @param strippedName file name without segment name prefix
     * @return action from the first matching rule, or {@link Action#DEFAULT}
     */
    public Action fileAction(String strippedName) {
      Path path;
      try {
        path = Path.of(strippedName);
      } catch (InvalidPathException e) {
        return Action.DEFAULT;
      }
      for (int i = 0; i < rules.size(); ++i) {
        for (PathMatcher matcher : matchers.get(i)) {
          if (matcher.matches(path)) {
            return rules.get(i).action();
          }
        }
      }
      return Action.DEFAULT;
    }
  }
}
