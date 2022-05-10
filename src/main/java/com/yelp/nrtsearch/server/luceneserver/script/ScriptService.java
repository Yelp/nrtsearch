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
package com.yelp.nrtsearch.server.luceneserver.script;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.config.ScriptCacheConfig;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.ScriptPlugin;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Class to manage to compilation of scripts. Scripting languages can be added through the addition
 * of {@link ScriptEngine}s, which are provided by {@link ScriptPlugin}s.
 *
 * <p>Compiles {@link Script} definitions into a factory that can be reused for all requests.
 *
 * <p>A cache is used to prevent re-compilation of identical scripts.
 */
public class ScriptService {

  private static ScriptService instance;

  private static final List<ScriptContext<?>> builtInContexts =
      ImmutableList.<ScriptContext<?>>builder()
          .add(FacetScript.CONTEXT)
          .add(ScoreScript.CONTEXT)
          .build();

  private final Map<String, ScriptEngine> scriptEngineMap = new HashMap<>();
  private final LoadingCache<ScriptCacheKey, Object> scriptCache;

  private ScriptService(LuceneServerConfiguration configuration) {
    // add provided javascript engine
    scriptEngineMap.put("js", new JsScriptEngine());

    ScriptCacheConfig scriptCacheConfig = configuration.getScriptCacheConfig();
    scriptCache =
        CacheBuilder.newBuilder()
            .concurrencyLevel(scriptCacheConfig.getConcurrencyLevel())
            .maximumSize(scriptCacheConfig.getMaximumSize())
            .expireAfterAccess(
                scriptCacheConfig.getExpirationTime(), scriptCacheConfig.getTimeUnit())
            .build(new ScriptLoader(this));
  }

  /**
   * Compile a given script into a factory for use with all requests.
   *
   * @param script definition of script
   * @param context context for script type to compile, defines the factory class type
   * @param <T> compiled factory type
   * @return compiled factory consistent with script context
   * @throws NullPointerException if script or context are null
   * @throws IllegalArgumentException if error getting factory from cache, or there is no {@link
   *     ScriptEngine} registered for the script lang
   */
  public <T> T compile(Script script, ScriptContext<T> context) {
    Objects.requireNonNull(script);
    Objects.requireNonNull(context);
    if (!scriptEngineMap.containsKey(script.getLang())) {
      throw new IllegalArgumentException(
          "Unable to find ScriptEngine for lang: " + script.getLang());
    }

    ScriptCacheKey cacheKey = new ScriptCacheKey(context, script.getLang(), script.getSource());
    try {
      Object factory = scriptCache.get(cacheKey);
      return context.factoryClazz.cast(factory);
    } catch (ExecutionException e) {
      throw new IllegalArgumentException(
          "Unable to get compiled script, source: "
              + script.getSource()
              + ", lang: "
              + script.getLang(),
          e);
    }
  }

  private void register(Iterable<ScriptEngine> scriptEngines) {
    scriptEngines.forEach(scriptEngine -> register(scriptEngine.getLang(), scriptEngine));
  }

  private void register(String name, ScriptEngine scriptEngine) {
    if (scriptEngineMap.containsKey(name)) {
      throw new IllegalArgumentException("ScriptEngine " + name + " already exists");
    }
    scriptEngineMap.put(name, scriptEngine);
  }

  /**
   * Initialize the script service. Registers any {@link ScriptEngine} provided by the given {@link
   * Plugin}.
   *
   * @param configuration server configuration
   * @param plugins loaded plugins
   */
  public static void initialize(LuceneServerConfiguration configuration, Iterable<Plugin> plugins) {
    instance = new ScriptService(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof ScriptPlugin) {
        ScriptPlugin scriptPlugin = (ScriptPlugin) plugin;
        instance.register(scriptPlugin.getScriptEngines(builtInContexts));
      }
    }
  }

  public static ScriptService getInstance() {
    return instance;
  }

  private static class ScriptCacheKey {
    private final ScriptContext<?> context;
    private final String lang;
    private final String source;

    ScriptCacheKey(ScriptContext<?> context, String lang, String source) {
      this.context = context;
      this.lang = lang;
      this.source = source;
    }

    @Override
    public int hashCode() {
      return Objects.hash(context, lang, source);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ScriptCacheKey) {
        ScriptCacheKey cacheKey = (ScriptCacheKey) o;
        return context == cacheKey.context
            && Objects.equals(lang, cacheKey.lang)
            && Objects.equals(source, cacheKey.source);
      }
      return false;
    }
  }

  /** Loader to compile script asynchronously when there is a cache miss. */
  static class ScriptLoader extends CacheLoader<ScriptCacheKey, Object> {

    private final ScriptService scriptService;

    ScriptLoader(ScriptService scriptService) {
      this.scriptService = scriptService;
    }

    @Override
    public Object load(ScriptCacheKey key) {
      ScriptEngine engine = scriptService.scriptEngineMap.get(key.lang);
      if (engine == null) {
        throw new IllegalArgumentException("Cannot compile script, unknown lang: " + key.lang);
      }
      Object factory = engine.compile(key.source, key.context);
      if (factory == null) {
        throw new IllegalArgumentException(
            "Compiled script is null, source: " + key.source + ", lang: " + key.lang);
      }
      return factory;
    }
  }
}
