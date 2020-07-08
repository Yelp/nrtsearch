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
package com.yelp.nrtsearch.server.plugins;

import com.yelp.nrtsearch.server.luceneserver.script.ScriptContext;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptEngine;
import java.util.Collections;
import java.util.List;

/**
 * Plugin interface that allows for the registration of new scripting languages. These {@link
 * ScriptEngine}s are invoked when their language is specified in a {@link
 * com.yelp.nrtsearch.server.grpc.Script} that is part of a query.
 */
public interface ScriptPlugin {

  /**
   * Provides custom {@link ScriptEngine} implementations for registration with the {@link
   * com.yelp.nrtsearch.server.luceneserver.script.ScriptService}. The service delegates compilation
   * of the script source to the appropriate engine based on the specified language.
   *
   * @param contexts list of possible script context types
   * @return list of provided {@link ScriptEngine} implementations
   */
  default Iterable<ScriptEngine> getScriptEngines(List<ScriptContext<?>> contexts) {
    return Collections.emptyList();
  }
}
