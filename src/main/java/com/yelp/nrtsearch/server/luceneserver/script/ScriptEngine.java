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

/**
 * Interface for a script engine capable of compiling a language into a factory suitable for the
 * given {@link ScriptContext}. This functionality can be accessed using the engine lang as the
 * language in a {@link com.yelp.nrtsearch.server.grpc.Script}.
 */
public interface ScriptEngine {
  String getLang();

  <T> T compile(String source, ScriptContext<T> context);
}
