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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = RegisterFieldsCommand.REGISTER_FIELDS,
    description = "Registers the fields for the specified index from the file")
public class RegisterFieldsCommand implements Callable<Integer> {
  public static final String REGISTER_FIELDS = "registerFields";

  @CommandLine.ParentCommand private LuceneClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-f", "--fileName"},
      description = "Name of the file containing fields to register to an index",
      required = true)
  private String fileName;

  public String getFileName() {
    return fileName;
  }

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      String jsonStr = Files.readString(Paths.get(getFileName()));
      client.registerFields(jsonStr);
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
