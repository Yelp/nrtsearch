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

/**
 * Exception thrown by {@link YamlConfigReader} functions when there is an unexpected problem. This
 * exception indicates malformed config, such as a component of the key lookup path not being a hash
 * or a value not being parsable into the desired type.
 */
public class ConfigReadException extends RuntimeException {
  ConfigReadException(String message) {
    super(message);
  }
}
