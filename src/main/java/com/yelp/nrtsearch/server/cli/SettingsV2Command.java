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
package com.yelp.nrtsearch.server.cli;

import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = SettingsV2Command.SETTINGS_V2,
    description =
        "Updates the settings for the specified index from the file if no settings specified gets the current settings")
public class SettingsV2Command implements Callable<Integer> {
  public static final String SETTINGS_V2 = "settingsV2";

  @CommandLine.ParentCommand private LuceneClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "Name of the index whose stats are to be retrieved",
      required = true)
  private String indexName;

  @CommandLine.Option(
      names = {"-f", "--fileName"},
      description = "Name of the file containing the settings to be updated")
  private String fileName;

  public String getIndexName() {
    return indexName;
  }

  public String getFileName() {
    return fileName;
  }

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      Path filePath = fileName == null ? null : Paths.get(getFileName());
      client.settingsV2(indexName, filePath);
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
