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
package com.yelp.nrtsearch.server.leader;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

/**
 * Context provided to {@link PrimaryElectorProvider}s when creating an elector.
 *
 * @param configuration server configuration
 * @param localInfo info describing the local node, epoch is unset (-1)
 * @param conditionalStore store supporting compare-and-set writes in the service bucket
 * @param prometheusRegistry metrics registry, may be null
 */
public record PrimaryElectorContext(
    NrtsearchConfig configuration,
    LeaderInfo localInfo,
    ConditionalObjectStore conditionalStore,
    PrometheusRegistry prometheusRegistry) {

  /** Service name of the cluster, the scope of leadership. */
  public String serviceName() {
    return configuration.getServiceName();
  }
}
