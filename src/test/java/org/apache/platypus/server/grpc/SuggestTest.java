/*
 *
 *  *
 *  *  Copyright 2019 Yelp Inc.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  *  either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *
 *
 *
 */

package org.apache.platypus.server.grpc;

import io.grpc.testing.GrpcCleanupRule;
import org.apache.platypus.server.luceneserver.GlobalState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.platypus.server.grpc.GrpcServer.rmDir;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class SuggestTest {
    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the
     * end of test.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    /**
     * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private GrpcServer grpcServer;
    static Path tempFile;

    @After
    public void tearDown() throws IOException {
        grpcServer.getGlobalState().close();
        rmDir(Paths.get(grpcServer.getRootDirName()));
        tempFile = null;
    }

    @Before
    public void setUp() throws IOException {
        String nodeName = "server1";
        String rootDirName = "server1RootDirName1";
        Path rootDir = folder.newFolder(rootDirName).toPath();
        String testIndex = "test_index";
        GlobalState globalState = new GlobalState(nodeName, rootDir, null, 9000, 9000);
        grpcServer = new GrpcServer(grpcCleanup, folder, false, globalState, rootDirName, testIndex, globalState.getPort());
        Path tempDir = folder.newFolder("TestSuggest").toPath();
        tempFile = tempDir.resolve("suggest.in");
    }

    @Test
    public void testInfixSuggest() throws Exception {
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
        Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("15\u001flove lost\u001ffoobar\n");
        out.close();

        BuildSuggestRequest.Builder buildSuggestRequestBuilder = BuildSuggestRequest.newBuilder();
        buildSuggestRequestBuilder.setSuggestName("suggest2");
        buildSuggestRequestBuilder.setIndexName("test_index");
        buildSuggestRequestBuilder.setInfixSuggester(InfixSuggester.newBuilder()
                .setAnalyzer("default").build());
        buildSuggestRequestBuilder.setLocalSource(SuggestLocalSource.newBuilder()
                .setLocalFile(tempFile.toAbsolutePath().toString())
                .setHasContexts(false).build());
        BuildSuggestResponse response = grpcServer.getBlockingStub().buildSuggest(buildSuggestRequestBuilder.build());
        assertEquals(1, response.getCount());

        SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
        suggestLookupBuilder.setText("lost");
        suggestLookupBuilder.setSuggestName("suggest2");
        suggestLookupBuilder.setIndexName("test_index");
        suggestLookupBuilder.setHighlight(true);
        SuggestLookupResponse suggestResponse = grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
        List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();
        assertEquals(15, results.get(0).getWeight());
        SuggestLookupHighlight suggestLookupHighLight = results.get(0).getSuggestLookupHighlight();
        assertEquals(2, suggestLookupHighLight.getOneHighlightList().size());
        assertEquals("love ", suggestLookupHighLight.getOneHighlight(0).getText());
        assertEquals(false, suggestLookupHighLight.getOneHighlight(0).getIsHit());
        assertEquals("lost", suggestLookupHighLight.getOneHighlight(1).getText());
        assertEquals(true, suggestLookupHighLight.getOneHighlight(1).getIsHit());
        assertEquals("foobar", results.get(0).getPayload());

        suggestLookupBuilder = SuggestLookupRequest.newBuilder();
        suggestLookupBuilder.setText("lo");
        suggestLookupBuilder.setSuggestName("suggest2");
        suggestLookupBuilder.setIndexName("test_index");
        suggestLookupBuilder.setHighlight(true);
        suggestResponse = grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
        results = suggestResponse.getResultsList();
        assertEquals(15, results.get(0).getWeight());
        suggestLookupHighLight = results.get(0).getSuggestLookupHighlight();
        assertEquals(5, suggestLookupHighLight.getOneHighlightList().size());
        assertEquals("lo", suggestLookupHighLight.getOneHighlight(0).getText());
        assertEquals(true, suggestLookupHighLight.getOneHighlight(0).getIsHit());
        assertEquals("ve", suggestLookupHighLight.getOneHighlight(1).getText());
        assertEquals(false, suggestLookupHighLight.getOneHighlight(1).getIsHit());
        assertEquals(" ", suggestLookupHighLight.getOneHighlight(2).getText());
        assertEquals(false, suggestLookupHighLight.getOneHighlight(2).getIsHit());
        assertEquals("lo", suggestLookupHighLight.getOneHighlight(3).getText());
        assertEquals(true, suggestLookupHighLight.getOneHighlight(3).getIsHit());
        assertEquals("st", suggestLookupHighLight.getOneHighlight(4).getText());
        assertEquals(false, suggestLookupHighLight.getOneHighlight(4).getIsHit());
        assertEquals("foobar", results.get(0).getPayload());
    }

}
