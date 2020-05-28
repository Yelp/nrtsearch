/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.yelp.nrtsearch.server.luceneserver.script;

import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.ScriptPlugin;
import com.yelp.nrtsearch.server.utils.LRUCache;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class to manage to compilation of scripts. Scripting languages can be added through the addition of
 * {@link ScriptEngine}s, which are provided by {@link ScriptPlugin}s.
 *
 * Compiles {@link Script} definitions into a factory that can be reused for all requests.
 *
 * A cache is used to prevent re-compilation of identical scripts.
 */
public class ScriptService {

    private static ScriptService instance;

    private final Map<String, ScriptEngine> scriptEngineMap = new HashMap<>();
    private final Object compileLock = new Object();
    private final LRUCache<ScriptCacheKey, Object> scriptCache;

    private ScriptService() {
        // add provided javascript engine
        scriptEngineMap.put("js", new JsScriptEngine());

        // TODO make configurable
        scriptCache = new LRUCache<>(1000);
    }

    /**
     * Compile a given script into a factory for use with all requests.
     * @param script definition of script
     * @param context context for script type to compile, defines the factory class type
     * @param <T> compiled factory type
     * @return compiled factory consistent with script context
     * @throws NullPointerException if the compiled factory, script, or context are null
     */
    public <T> T compile(Script script, ScriptContext<T> context) {
        Objects.requireNonNull(script);
        Objects.requireNonNull(context);
        if (!scriptEngineMap.containsKey(script.getLang())) {
            throw new IllegalArgumentException("Unable to find ScriptEngine for lang: " + script.getLang());
        }

        ScriptCacheKey cacheKey = new ScriptCacheKey(context.name, script.getLang(), script.getSource());
        Object factory = scriptCache.get(cacheKey);
        if (factory == null) {
            // lock compilation to ensure it only happens once
            synchronized (compileLock) {
                // verify the script was not compiled while waiting on the lock
                factory = scriptCache.get(cacheKey);
                if (factory == null) {
                    factory = scriptEngineMap.get(script.getLang()).compile(script.getSource(), context);
                    Objects.requireNonNull(factory);
                    scriptCache.put(cacheKey, factory);
                }
            }
        }
        return context.factoryClazz.cast(factory);
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
     * Initialize the script service. Registers any {@link ScriptEngine} provided by the given {@link Plugin}.
     * @param plugins loaded plugins
     */
    public static void initialize(Iterable<Plugin> plugins) {
        instance = new ScriptService();
        for (Plugin plugin : plugins) {
            if (plugin instanceof ScriptPlugin) {
                ScriptPlugin scriptPlugin = (ScriptPlugin) plugin;
                instance.register(scriptPlugin.getScriptEngines());
            }
        }
    }

    public static ScriptService getInstance() {
        return instance;
    }

    private static class ScriptCacheKey {
        private final String context;
        private final String lang;
        private final String source;

        ScriptCacheKey(String context, String lang, String source) {
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
                return Objects.equals(context, cacheKey.context)
                        && Objects.equals(lang, cacheKey.lang)
                        && Objects.equals(source, cacheKey.source);
            }
            return false;
        }
    }
}
