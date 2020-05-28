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

/**
 * Context that holds type information needed to compile a script. Provides the factory class
 * required by the script type.
 * @param <T> factory class type
 */
public class ScriptContext<T> {
    public final String name;
    public final Class<T> factoryClazz;

    public ScriptContext(String name, Class<T> factoryClazz) {
        this.name = name;
        this.factoryClazz = factoryClazz;
    }
}
