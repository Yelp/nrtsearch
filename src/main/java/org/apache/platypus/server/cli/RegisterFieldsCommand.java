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

@CommandLine.Command(name = RegisterFieldsCommand.REGISTER_FIELDS, mixinStandardHelpOptions = true, version = "registerFields 0.1",
        description = "registers the fields for the specified index from the file")
public class RegisterFieldsCommand {
    public static final String REGISTER_FIELDS = "registerFields";

    @CommandLine.Option(names = {"-f", "--fileName"}, description = "name of the file containing fields to register to an index", required = true)
    private String fileName;

    public String getFileName() {
        return fileName;
    }
}