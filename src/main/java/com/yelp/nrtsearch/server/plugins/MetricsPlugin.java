/*
 * Copyright 2022 Yelp Inc.
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

import io.prometheus.client.CollectorRegistry;

/**
 * Plugin interface that allows plugin to register their prometheus metrics in Nrtsearch prometheus
 * collector registry.
 */
public interface MetricsPlugin {

  /** @param collectorRegistry Nrtsearch Prometheus collector registry. */
  void registerMetrics(CollectorRegistry collectorRegistry);
}
