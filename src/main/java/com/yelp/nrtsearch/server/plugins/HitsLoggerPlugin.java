/*
 * Copyright 2024 Yelp Inc.
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

import com.yelp.nrtsearch.server.luceneserver.logging.HitsLogger;

/**
 * Plugin interface for providing custom logging. The plugins will be loaded at the startup time of
 * the {@link com.yelp.nrtsearch.server.grpc.LuceneServer}. The hits logger instance provided from
 * the getHitsLogger() will be responsible for handling the corresponding hits logger task.
 * Therefore, do not alter the instance object for any requests.
 */
public interface HitsLoggerPlugin {
  default HitsLogger getHitsLogger() {
    return null;
  }
}
