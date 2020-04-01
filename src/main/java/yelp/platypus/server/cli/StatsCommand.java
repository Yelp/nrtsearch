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

package yelp.platypus.server.cli;

import picocli.CommandLine;


@CommandLine.Command(name = StatsCommand.STATS, mixinStandardHelpOptions = true, version = "stats 0.1",
        description = "Retrieve index statistics")
public class StatsCommand {
    public static final String STATS = "stats";

    @CommandLine.Option(names = {"-i", "--indexName"}, description = "name of the index whose stats are to be retrieved", required = true)
    private String indexName;

    public String getIndexName() {
        return indexName;
    }

}