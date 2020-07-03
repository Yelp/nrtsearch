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

import static com.yelp.nrtsearch.server.cli.LuceneClientCommand.logger;

import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.grpc.SearcherVersion;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = WriteNRTPointCommand.WRITE_NRT_POINT,
    description = "Write NRT index to make all recent index operations searchable")
public class WriteNRTPointCommand implements Callable<Integer> {
  public static final String WRITE_NRT_POINT = "writeNRT";

  @CommandLine.ParentCommand private LuceneClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "Name of the index whose NRT point is to be updated",
      required = true)
  private String indexName;

  public String getIndexName() {
    return indexName;
  }

  @CommandLine.Option(
      names = {"--host"},
      description = "Primary host name (default: ${DEFAULT-VALUE})",
      defaultValue = "localhost")
  private String hostName;

  public String getHostName() {
    return hostName;
  }

  @CommandLine.Option(
      names = {"-p", "--port"},
      description = "Primary replication port number",
      required = true)
  private String port;

  public int getPort() {
    return Integer.parseInt(port);
  }

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      ReplicationServerClient replicationServerClient =
          new ReplicationServerClient(getHostName(), getPort());
      SearcherVersion searcherVersion = replicationServerClient.writeNRTPoint(getIndexName());
      logger.info("didRefresh: " + searcherVersion.getDidRefresh());
      logger.info("searcherVersion: " + searcherVersion.getVersion());
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
