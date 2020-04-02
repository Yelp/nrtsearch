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

@CommandLine.Command(name = LiveSettingsCommand.LIVE_SETTINGS, mixinStandardHelpOptions = true, version = "liveSettings 0.1",
        description = "updates the lives settings for the the specified index")
public class LiveSettingsCommand {
    public static final String LIVE_SETTINGS = "liveSettings";

    @CommandLine.Option(names = {"-i", "--indexName"}, description = "name of the index whose live settings are to be updated", required = true)
    private String indexName;

    @CommandLine.Option(names = {"--maxRefreshSec"}, description = "Longest time to wait before reopening IndexSearcher (i.e., periodic background reopen).")
    private double maxRefreshSec = 1.0;

    @CommandLine.Option(names = {"--minRefreshSec"}, description = "Shortest time to wait before reopening IndexSearcher (i.e., when a search is waiting for a specific indexGen).")
    private double minRefreshSec = 0.5;

    @CommandLine.Option(names = {"--maxSearcherAgeSec"}, description = "Non-current searchers older than this are pruned.")
    private double maxSearcherAgeSec = 60.0;

    @CommandLine.Option(names = {"--indexRamBufferSizeMB"}, description = "Size (in MB) of IndexWriter's RAM buffer.")
    private double indexRamBufferSizeMB = 250;

    public String getIndexName() {
        return indexName;
    }

    public double getMaxRefreshSec() {
        return maxRefreshSec;
    }

    public double getMinRefreshSec() {
        return minRefreshSec;
    }

    public double getMaxSearcherAgeSec() {
        return maxSearcherAgeSec;
    }

    public double getIndexRamBufferSizeMB() {
        return indexRamBufferSizeMB;
    }
}