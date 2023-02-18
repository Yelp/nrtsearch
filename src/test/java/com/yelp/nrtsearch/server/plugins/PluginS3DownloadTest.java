/*
 * Copyright 2023 Yelp Inc.
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
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.CustomRequest;
import com.yelp.nrtsearch.server.grpc.CustomResponse;
import com.yelp.nrtsearch.test_utils.NrtsearchTest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test that a plugin can be downloaded from S3 and loaded in Nrtsearch when it's complete S3 path
 * is provided in the config file.
 */
public class PluginS3DownloadTest extends NrtsearchTest {

  public PluginS3DownloadTest() throws IOException {}

  private static final String PLUGIN_S3_KEY = "nrtsearch/plugins/example-plugin-0.0.1.zip";

  @BeforeClass
  public static void addPluginToS3() {
    getS3Client()
        .putObject(
            getS3BucketName(),
            PLUGIN_S3_KEY,
            Paths.get(
                    PluginS3DownloadTest.class
                        .getClassLoader()
                        .getResource("util/example-plugin-0.0.1.zip")
                        .getPath())
                .toFile());
  }

  @Override
  protected List<String> getPlugins() {
    String path = String.format("s3://%s/%s", getS3BucketName(), PLUGIN_S3_KEY);
    return List.of(path);
  }

  @Test
  public void testPluginS3Path() {
    assertTrue(Files.exists(getPluginSearchPath().resolve("example-plugin-0.0.1")));
    CustomResponse response =
        getClient()
            .getBlockingStub()
            .custom(
                CustomRequest.newBuilder()
                    .setId("custom_analyzers")
                    .setPath("get_available_analyzers")
                    .build());
    assertEquals("plugin_analyzer", response.getResponseOrThrow("available_analyzers"));
  }
}
