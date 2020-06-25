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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PluginMetadataTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testParsesConfig() {
    String config =
        String.join(
            "\n",
            "name: plugin_name",
            "version: 1.0.0",
            "description: some plugin description",
            "classname: com.name.plugin.class");
    PluginMetadata metadata =
        PluginMetadata.fromInputStream(new ByteArrayInputStream(config.getBytes()));
    assertEquals("plugin_name", metadata.getName());
    assertEquals("1.0.0", metadata.getVersion());
    assertEquals("some plugin description", metadata.getDescription());
    assertEquals("com.name.plugin.class", metadata.getClassname());
  }

  @Test
  public void testParsesConfigWithExtraKeys() {
    String config =
        String.join(
            "\n",
            "name: plugin_name",
            "version: 1.0.0",
            "description: some plugin description",
            "classname: com.name.plugin.class",
            "extra: some value");
    PluginMetadata metadata =
        PluginMetadata.fromInputStream(new ByteArrayInputStream(config.getBytes()));
    assertEquals("plugin_name", metadata.getName());
    assertEquals("1.0.0", metadata.getVersion());
    assertEquals("some plugin description", metadata.getDescription());
    assertEquals("com.name.plugin.class", metadata.getClassname());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequiresName() {
    String config =
        String.join(
            "\n",
            "version: 1.0.0",
            "description: some plugin description",
            "classname: com.name.plugin.class");
    PluginMetadata.fromInputStream(new ByteArrayInputStream(config.getBytes()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequiresVersion() {
    String config =
        String.join(
            "\n",
            "name: plugin_name",
            "description: some plugin description",
            "classname: com.name.plugin.class");
    PluginMetadata.fromInputStream(new ByteArrayInputStream(config.getBytes()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequiresDescription() {
    String config =
        String.join(
            "\n", "name: plugin_name", "version: 1.0.0", "classname: com.name.plugin.class");
    PluginMetadata.fromInputStream(new ByteArrayInputStream(config.getBytes()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequiresClassname() {
    String config =
        String.join(
            "\n", "name: plugin_name", "version: 1.0.0", "description: some plugin description");
    PluginMetadata.fromInputStream(new ByteArrayInputStream(config.getBytes()));
  }

  @Test
  public void testFindsMetadataFile() throws IOException {
    File metadataFile = folder.newFile(PluginMetadata.METADATA_FILE);
    assertEquals(metadataFile, PluginMetadata.getMetadataFile(folder.getRoot()));
  }

  @Test(expected = FileNotFoundException.class)
  public void testMetadataFileNotFound() throws IOException {
    PluginMetadata.getMetadataFile(folder.getRoot());
  }
}
