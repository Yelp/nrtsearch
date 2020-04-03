/*
 *
 *  * Copyright 2019 Yelp Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  * either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.yelp.nrtsearch.server.cli;

import picocli.CommandLine;

@CommandLine.Command(name = GetCurrentSearcherVersion.CURRENT_SEARCHER_VERSION, mixinStandardHelpOptions = true, version = "currSearcherVer 0.1",
        description = "Gets the most recent searcher version on replica, should match primary version returned on the last writeNRT call")
public class GetCurrentSearcherVersion {
    public static final String CURRENT_SEARCHER_VERSION = "currSearcherVer";

    @CommandLine.Option(names = {"-i", "--indexName"}, description = "name of the index whose NRT point is to be updated", required = true)
    private String indexName;

    public String getIndexName() {
        return indexName;
    }

    @CommandLine.Option(names = {"--host"}, description = "replica host name", required = false)
    private String hostName = "localhost";

    public String getHostName() {
        return hostName;
    }

    @CommandLine.Option(names = {"-p", "--port"}, description = "replica replication port number", required = true)
    private String port;

    public int getPort() {
        return Integer.parseInt(port);
    }

}
