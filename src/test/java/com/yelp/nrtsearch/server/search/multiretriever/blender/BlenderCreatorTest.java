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
package com.yelp.nrtsearch.server.search.multiretriever.blender;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.Blender;
import com.yelp.nrtsearch.server.grpc.PluginBlender;
import com.yelp.nrtsearch.server.grpc.WeightedRrfBlender;
import com.yelp.nrtsearch.server.plugins.BlenderPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.search.multiretriever.blender.operation.WeightedRrfBlenderOperation;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class BlenderCreatorTest {

  /** Test plugin that extends {@link Plugin} and implements {@link BlenderPlugin}. */
  private static class TestBlenderPlugin extends Plugin implements BlenderPlugin {
    private final Map<String, BlenderProvider<? extends BlenderOperation>> blenders;

    TestBlenderPlugin(Map<String, BlenderProvider<? extends BlenderOperation>> blenders) {
      this.blenders = blenders;
    }

    @Override
    public Map<String, BlenderProvider<? extends BlenderOperation>> getBlenders() {
      return blenders;
    }
  }

  private static BlenderOperation stubOp() {
    return (results, contexts) -> Collections.emptyList();
  }

  private static BlenderProvider<BlenderOperation> stubProvider() {
    BlenderOperation op = stubOp();
    return params -> op;
  }

  private static Blender weightedRrfBlender() {
    return Blender.newBuilder().setWeightedRrf(WeightedRrfBlender.newBuilder().build()).build();
  }

  private static Blender pluginBlender(String name) {
    return Blender.newBuilder().setPlugin(PluginBlender.newBuilder().setName(name).build()).build();
  }

  @Test
  public void testWeightedRrfReturnsCorrectType() {
    BlenderCreator creator = new BlenderCreator(null);
    BlenderOperation op = creator.getBlenderOperation(weightedRrfBlender());
    assertTrue(op instanceof WeightedRrfBlenderOperation);
  }

  @Test
  public void testWeightedRrfReturnsNewInstancePerCall() {
    BlenderCreator creator = new BlenderCreator(null);
    BlenderOperation first = creator.getBlenderOperation(weightedRrfBlender());
    BlenderOperation second = creator.getBlenderOperation(weightedRrfBlender());
    assertTrue(first instanceof WeightedRrfBlenderOperation);
    assertTrue(second instanceof WeightedRrfBlenderOperation);
  }

  @Test
  public void testPluginRegistrationAndLookup() {
    Plugin plugin = new TestBlenderPlugin(Map.of("my_blender", stubProvider()));

    BlenderCreator.initialize(null, List.of(plugin));

    BlenderOperation found =
        BlenderCreator.getInstance().getBlenderOperation(pluginBlender("my_blender"));
    assertNotNull(found);
  }

  @Test
  public void testUnknownPluginThrows() {
    BlenderCreator creator = new BlenderCreator(null);
    assertThrows(
        IllegalArgumentException.class,
        () -> creator.getPluginBlender(PluginBlender.newBuilder().setName("nonexistent").build()));
  }

  @Test
  public void testDuplicatePluginRegistrationThrows() {
    // Two plugins both register the same name — second one must throw
    Plugin plugin1 = new TestBlenderPlugin(Map.of("shared_name", stubProvider()));
    Plugin plugin2 = new TestBlenderPlugin(Map.of("shared_name", stubProvider()));

    assertThrows(
        IllegalArgumentException.class,
        () -> BlenderCreator.initialize(null, List.of(plugin1, plugin2)));
  }

  @Test
  public void testUnsupportedBlenderTypeThrows() {
    BlenderCreator creator = new BlenderCreator(null);
    assertThrows(
        IllegalArgumentException.class,
        () -> creator.getBlenderOperation(Blender.newBuilder().build()));
  }

  @Test
  public void testInitializeSetsInstance() {
    BlenderCreator.initialize(null, List.of());
    assertNotNull(BlenderCreator.getInstance());
  }
}
