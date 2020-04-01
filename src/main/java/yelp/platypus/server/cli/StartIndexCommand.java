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

import static yelp.platypus.server.cli.StartIndexCommand.START_INDEX;


@CommandLine.Command(name = START_INDEX, mixinStandardHelpOptions = true, version = "startIndex 0.1",
        description = "starts the index")
public class StartIndexCommand {
    public static final String START_INDEX = "startIndex";

    @CommandLine.Option(names = {"-f", "--fileName"}, description = "name of the file containing the start index specifics", required = true)
    private String fileName;

    public String getFileName() {
        return fileName;
    }
}
