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

import static org.apache.platypus.server.cli.AddDocumentsCommand.ADD_DOCUMENTS;

@CommandLine.Command(name = ADD_DOCUMENTS, mixinStandardHelpOptions = true, version = "addDocuments 0.1",
        description = "add document to the index")
public class AddDocumentsCommand {
    public static final String ADD_DOCUMENTS = "addDocuments";

    @CommandLine.Option(names = {"-f", "--fileName"}, description = "documents to be added in a csv format. First row has names of fields and rows after are values", required = true)
    private String fileName;

    public String getFileName() {
        return fileName;
    }

    @CommandLine.Option(names = {"-i", "--indexName"}, description = "name of the index to add documents to", required = true)
    private String indexName;

    public String getIndexName() {
        return indexName;
    }

}
