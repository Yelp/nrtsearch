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

@CommandLine.Command(name = WriteNRTPointCommand.WRITE_NRT_POINT, mixinStandardHelpOptions = true, version = "writeNRT 0.1",
        description = "Write NRT index to make all recent index operations searchable")
public class WriteNRTPointCommand {
    public static final String WRITE_NRT_POINT = "writeNRT";

    @CommandLine.Option(names = {"-i", "--indexName"}, description = "name of the index whose NRT point is to be updated", required = true)
    private String indexName;

    public String getIndexName() {
        return indexName;
    }

    @CommandLine.Option(names = {"--host"}, description = "primary host name", required = false)
    private String hostName = "localhost";

    public String getHostName() {
        return hostName;
    }

    @CommandLine.Option(names = {"-p", "--port"}, description = "primary replication port number", required = true)
    private String port;

    public int getPort() {
        return Integer.parseInt(port);
    }

}
