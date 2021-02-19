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
package com.yelp.nrtsearch.server.luceneserver.rescore;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.PluginRescorer;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.RescorerPlugin;
import com.yelp.nrtsearch.server.utils.StructValueTransformer;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.search.Rescorer;

public class RescorerCreator {

  private static RescorerCreator instance;

  private final Map<String, RescorerProvider<? extends Rescorer>> rescorersMap = new HashMap<>();

  public RescorerCreator(LuceneServerConfiguration configuration) {}

  public Rescorer createRescorer(PluginRescorer grpcPluginRescorer) {
    RescorerProvider<?> provider = rescorersMap.get(grpcPluginRescorer.getName());
    if (provider == null) {
      throw new IllegalArgumentException(
          "Invalid rescorer name: "
              + grpcPluginRescorer.getName()
              + ", must be one of: "
              + rescorersMap.keySet());
    }
    return provider.get(StructValueTransformer.transformStruct(grpcPluginRescorer.getParams()));
  }

  private void register(Map<String, RescorerProvider<? extends Rescorer>> rescorers) {
    rescorers.forEach(this::register);
  }

  private void register(String name, RescorerProvider<? extends Rescorer> rescorer) {
    if (rescorersMap.containsKey(name)) {
      throw new IllegalArgumentException("Rescorer " + name + " already exists");
    }
    rescorersMap.put(name, rescorer);
  }

  public static void initialize(LuceneServerConfiguration configuration, Iterable<Plugin> plugins) {
    instance = new RescorerCreator(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof RescorerPlugin) {
        RescorerPlugin rescorePlugin = (RescorerPlugin) plugin;
        instance.register(rescorePlugin.getRescorers());
      }
    }
  }

  public static RescorerCreator getInstance() {
    return instance;
  }
}
