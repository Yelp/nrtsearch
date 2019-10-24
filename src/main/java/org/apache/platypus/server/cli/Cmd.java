/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.platypus.server.cli;

import picocli.CommandLine;

/*Each new command needs to be added here*/
@CommandLine.Command(name = "cmd", synopsisSubcommandLabel = "COMMAND", subcommands = {
        CreateIndexCommand.class,
        LiveSettingsCommand.class,
        RegisterFieldsCommand.class,
        SettingsCommand.class,
        StartIndexCommand.class,
        AddDocumentsCommand.class,
        RefreshCommand.class,
        CommitCommand.class,
        StatsCommand.class,
        SearchCommand.class,
        DeleteDocumentsCommand.class
})
public class Cmd {
    @CommandLine.Option(names = {"-p", "--port"}, description = "port number of server to connect to", required = true)
    private String port;

    public int getPort() {
        return Integer.parseInt(port);
    }

}
