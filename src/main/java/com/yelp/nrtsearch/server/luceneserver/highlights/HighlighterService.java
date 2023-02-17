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
package com.yelp.nrtsearch.server.luceneserver.highlights;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.plugins.HighlighterPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.util.HashMap;
import java.util.Map;

/** Factory class that handles registration and creation of {@link Highlighter}s. */
public class HighlighterService {

  private static HighlighterService instance;
  private final Map<String, Highlighter> highlighterInstanceMap = new HashMap<>();

  /**
   * Constructor.
   *
   * @param configuration server configuration
   */
  public HighlighterService(LuceneServerConfiguration configuration) {}

  private void register(Iterable<Highlighter> highlighters) {
    highlighters.forEach(highlighter -> register(highlighter.getName(), highlighter));
  }

  private void register(String name, Highlighter highlighter) {
    if (highlighterInstanceMap.containsKey(name)) {
      throw new IllegalArgumentException("Highlighter " + name + " already exists");
    }
    highlighterInstanceMap.put(name, highlighter);
  }

  /** builtin highlighters are not shipped as plugins. They will be initialized here. */
  private static void initializeBuiltinHighlighters() {
    NRTFastVectorHighlighter nrtFastVectorHighlighter = NRTFastVectorHighlighter.getInstance();
    instance.register(nrtFastVectorHighlighter.getName(), nrtFastVectorHighlighter);
  }
  /**
   * Initialize singleton instance of {@link HighlighterService}. Registers all builtin highlighter
   * and any additional highlighter provided by {@link HighlighterPlugin}s.
   *
   * @param configuration service configuration
   * @param plugins list of loaded plugins
   */
  public static void initialize(LuceneServerConfiguration configuration, Iterable<Plugin> plugins) {
    instance = new HighlighterService(configuration);
    initializeBuiltinHighlighters();
    for (Plugin plugin : plugins) {
      if (plugin instanceof HighlighterPlugin) {
        HighlighterPlugin highlighterPlugin = (HighlighterPlugin) plugin;
        instance.register(highlighterPlugin.getHighlighters());
      }
    }
  }

  /** Get singleton instance. */
  public static HighlighterService getInstance() {
    return instance;
  }

  /**
   * Fetch the corresponding highlighter based on the highlighter name.
   *
   * @param highlighterName the grpc highlight setting
   * @return the highlighter specified by the name in the highlight setting
   */
  public Highlighter getHighlighter(String highlighterName) {
    Highlighter highlighter = highlighterInstanceMap.get(highlighterName);
    if (highlighter == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown highlighter name [%s] is specified; The available highlighters are %s",
              highlighterName, highlighterInstanceMap.keySet()));
    }
    return highlighter;
  }
}
