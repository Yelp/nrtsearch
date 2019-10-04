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

@CommandLine.Command(name = SettingsCommand.SETTINGS, mixinStandardHelpOptions = true, version = "settings 0.1",
        description = "updates the settings for the specified index from the file if no settings specified gets the current settings")
public class SettingsCommand {
    public static final String SETTINGS = "settings";

    @CommandLine.Option(names = {"-f", "--fileName"}, description = "name of the file containing the settings to be updated", required = true)
    private String fileName;

    public String getFileName() {
        return fileName;
    }
}