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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;

import com.google.gson.Gson;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class NrtsearchClientBuilderTest {

  @Test
  public void buildRequest() throws IOException {
    Path filePath = Paths.get("src", "test", "resources", "addDocs.txt");
    int maxBufferLen = 10;
    NrtsearchClientBuilder.AddJsonDocumentsClientBuilder addJsonDocumentsClientBuilder =
        new NrtsearchClientBuilder.AddJsonDocumentsClientBuilder(
            "test_index", new Gson(), filePath, maxBufferLen);
    Stream<AddDocumentRequest> addDocumentRequestStream =
        addJsonDocumentsClientBuilder.buildRequest(filePath);
    List<AddDocumentRequest> addDocumentRequestList =
        addDocumentRequestStream.collect(Collectors.toList());
    AddDocumentRequest firstDoc = addDocumentRequestList.get(0);
    Assertions.assertThat(firstDoc.getIndexNamesList()).containsExactly("test_index");
    assertEquals("first vendor", firstDoc.getFieldsMap().get("vendor_name").getValue(0));
    assertEquals("first again", firstDoc.getFieldsMap().get("vendor_name").getValue(1));
    assertEquals("3", firstDoc.getFieldsMap().get("count").getValue(0));
    AddDocumentRequest secondDoc = addDocumentRequestList.get(1);
    Assertions.assertThat(secondDoc.getIndexNamesList()).containsExactly("test_index");
    assertEquals("second vendor", secondDoc.getFieldsMap().get("vendor_name").getValue(0));
    assertEquals("second again", secondDoc.getFieldsMap().get("vendor_name").getValue(1));
    assertEquals("7", secondDoc.getFieldsMap().get("count").getValue(0));
  }
}
