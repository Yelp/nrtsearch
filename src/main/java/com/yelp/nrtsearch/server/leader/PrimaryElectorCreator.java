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

import com.yelp.nrtsearch.server.config.LeaderElectionConfig;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.PrimaryElectorPlugin;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Registry for {@link PrimaryElector} types. The built-in {@code s3_lease} type is always
 * registered. Plugins implementing {@link PrimaryElectorPlugin} may register additional types.
 */
public class PrimaryElectorCreator {
  private static PrimaryElectorCreator instance;

  private final Map<String, PrimaryElectorProvider> providers = new HashMap<>();

  private PrimaryElectorCreator() {
    providers.put(
        S3LeaseElector.TYPE,
        (context, params) -> {
          if (!(context.remoteBackend() instanceof S3Backend s3Backend)) {
            throw new IllegalArgumentException(
                S3LeaseElector.TYPE + " leader election requires an S3 remote backend");
          }
          ConditionalObjectStore store =
              new S3ConditionalObjectStore(s3Backend.getS3(), s3Backend.getServiceBucket());
          return S3LeaseElector.fromParams(store, context, params);
        });
  }

  private void register(String name, PrimaryElectorProvider provider) {
    if (LeaderElectionConfig.NONE.equals(name)) {
      throw new IllegalArgumentException("PrimaryElector type name is reserved: " + name);
    }
    if (providers.containsKey(name)) {
      throw new IllegalArgumentException("PrimaryElector " + name + " already exists");
    }
    providers.put(name, provider);
  }

  /**
   * Create the elector configured by {@code leaderElection}, or empty if election is disabled.
   *
   * @param context elector context
   * @throws IllegalArgumentException if the configured type is not registered
   */
  public Optional<PrimaryElector> createElector(PrimaryElectorContext context) {
    LeaderElectionConfig config = context.configuration().getLeaderElectionConfig();
    if (!config.isEnabled()) {
      return Optional.empty();
    }
    PrimaryElectorProvider provider = providers.get(config.getType());
    if (provider == null) {
      throw new IllegalArgumentException(
          "Unknown leader election type: "
              + config.getType()
              + ", must be one of: "
              + providers.keySet());
    }
    return Optional.of(provider.create(context, config.getParams()));
  }

  /**
   * Initialize the singleton instance, registering types from {@link PrimaryElectorPlugin}s.
   *
   * @param configuration server configuration
   * @param plugins loaded plugins
   */
  public static void initialize(NrtsearchConfig configuration, Iterable<Plugin> plugins) {
    instance = new PrimaryElectorCreator();
    for (Plugin plugin : plugins) {
      if (plugin instanceof PrimaryElectorPlugin electorPlugin) {
        electorPlugin.getPrimaryElectors().forEach(instance::register);
      }
    }
  }

  /** Get the singleton instance. */
  public static PrimaryElectorCreator getInstance() {
    return instance;
  }
}
