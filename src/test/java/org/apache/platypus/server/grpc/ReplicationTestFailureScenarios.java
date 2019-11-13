package org.apache.platypus.server.grpc;

import com.google.gson.Gson;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.platypus.server.luceneserver.GlobalState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.apache.platypus.server.grpc.GrpcServer.rmDir;
import static org.apache.platypus.server.grpc.LuceneServerTest.RETRIEVED_VALUES;
import static org.apache.platypus.server.grpc.LuceneServerTest.checkHits;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class ReplicationTestFailureScenarios {
    public static final String TEST_INDEX = "test_index";
    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the
     * end of test.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    /**
     * This rule ensure the temporary folder which maintains stateDir are cleaned up after each test
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private GrpcServer luceneServerPrimary;
    private GrpcServer replicationServerPrimary;

    private GrpcServer luceneServerSecondary;
    private GrpcServer replicationServerSecondary;

    @After
    public void tearDown() throws IOException {
        luceneServerPrimary.getGlobalState().close();
        luceneServerSecondary.getGlobalState().close();
        rmDir(Paths.get(luceneServerPrimary.getRootDirName()));
        rmDir(Paths.get(luceneServerSecondary.getRootDirName()));
    }

    @Before
    public void setUp() throws IOException {
        startPrimaryServer();
        startSecondaryServer();
    }

    public void startPrimaryServer() throws IOException {
        //set up primary servers
        String nodeNamePrimary = "serverPrimary";
        String rootDirNamePrimary = "serverPrimaryRootDirName";
        rmDir(Paths.get(folder.getRoot().toString(), rootDirNamePrimary));
        Path rootDirPrimary = folder.newFolder(rootDirNamePrimary).toPath();
        String testIndex = TEST_INDEX;
        GlobalState globalStatePrimary = new GlobalState(nodeNamePrimary, rootDirPrimary, "localhost", 9900, 9001);
        luceneServerPrimary = new GrpcServer(grpcCleanup, folder, false, globalStatePrimary, nodeNamePrimary, testIndex, globalStatePrimary.getPort());
        replicationServerPrimary = new GrpcServer(grpcCleanup, folder, true, globalStatePrimary, nodeNamePrimary, testIndex, 9001);

    }

    public void startSecondaryServer() throws IOException {
        //set up secondary servers
        String nodeNameSecondary = "serverSecondary";
        String rootDirNameSecondary = "serverSecondaryRootDirName";
        rmDir(Paths.get(folder.getRoot().toString(), rootDirNameSecondary));
        Path rootDirSecondary = folder.newFolder(rootDirNameSecondary).toPath();
        String testIndex = TEST_INDEX;
        GlobalState globalStateSecondary = new GlobalState(nodeNameSecondary, rootDirSecondary, "localhost", 9902, 9003);
        luceneServerSecondary = new GrpcServer(grpcCleanup, folder, false, globalStateSecondary, nodeNameSecondary, testIndex, globalStateSecondary.getPort());
        replicationServerSecondary = new GrpcServer(grpcCleanup, folder, true, globalStateSecondary, nodeNameSecondary, testIndex, 9003);
    }

    public void shutdownPrimaryServer() throws IOException {
        luceneServerPrimary.getGlobalState().close();
        rmDir(Paths.get(luceneServerPrimary.getRootDirName()));
        luceneServerPrimary.shutdown();
        replicationServerPrimary.shutdown();

    }

    public void shutdownSecondaryServer() throws IOException {
        luceneServerSecondary.getGlobalState().close();
        rmDir(Paths.get(luceneServerSecondary.getRootDirName()));
        luceneServerSecondary.shutdown();
        replicationServerSecondary.shutdown();
    }

    @Test
    public void replicaStoppedWhenPrimaryIndexing() throws IOException, InterruptedException {
        //startIndex Primary
        GrpcServer.TestServer testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
        //startIndex replica
        GrpcServer.TestServer testServerReplica = new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);

        //add 2 docs to primary
        testServerPrimary.addDocuments();

        //refresh (also sends NRTPoint to replicas)
        luceneServerPrimary.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

        //stop replica instance
        shutdownSecondaryServer();

        //add 2 docs to primary
        testServerPrimary.addDocuments();

        //re-start replica instance from a fresh index state i.e. empty index dir
        startSecondaryServer();
        //startIndex replica
        testServerReplica = new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);

        //add 2 more docs (6 total now), annoying, sendNRTPoint gets called from primary only upon a flush i.e. an index operation
        testServerPrimary.addDocuments();

        // publish new NRT point (retrieve the current searcher version on primary)
        publishNRTAndValidateSearchResults(6);

    }

    @Test
    public void primaryStoppedWhenIndexing() throws IOException, InterruptedException {
        //startIndex Primary
        GrpcServer.TestServer testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
        //startIndex replica
        GrpcServer.TestServer testServerReplica = new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
        //add 2 docs to primary
        testServerPrimary.addDocuments();
        //add 2 docs to primary
        testServerPrimary.addDocuments();
        //commit primary
        luceneServerPrimary.getBlockingStub().commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());
        //commit secondary
        luceneServerSecondary.getBlockingStub().commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());
        //both primary and replica should have 4 docs
        publishNRTAndValidateSearchResults(4);

        //non-graceful primary shutdown (i.e. blow away index directory)
        shutdownPrimaryServer();
        //start primary again with new empty index directory
        startPrimaryServer();
        //startIndex Primary with primaryGen = 1
        testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY, 1);
        //stop and restart secondary to connect to "new" primary
        gracefullRestartSecondary();
        //add 2 docs to primary
        testServerPrimary.addDocuments();
        publishNRTAndValidateSearchResults(2);

    }

    @Test
    public void primaryStoppedGracefully() throws IOException, InterruptedException {
        //startIndex primary
        GrpcServer.TestServer testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
        //startIndex replica
        GrpcServer.TestServer testServerReplica = new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
        //add 2 docs to primary
        testServerPrimary.addDocuments();
        //commit primary
        luceneServerPrimary.getBlockingStub().commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());
        //commit secondary
        luceneServerSecondary.getBlockingStub().commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());
        //both primary and replica should have 2 docs
        publishNRTAndValidateSearchResults(2);

        //gracefully stop and restart primary
        gracefullRestartPrimary(1);
        //stop and restart secondary to connect to "new" primary
        gracefullRestartSecondary();
        //add 2 docs to primary
        testServerPrimary.addDocuments();
        publishNRTAndValidateSearchResults(4);

    }

    private void gracefullRestartSecondary() {
        luceneServerSecondary.getBlockingStub().stopIndex(StopIndexRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .build());
        luceneServerSecondary.getBlockingStub().startIndex(StartIndexRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setMode(Mode.REPLICA)
                .setPrimaryAddress("localhost")
                .setPort(9001) //primary port for replication server
                .build());
    }

    private void gracefullRestartPrimary(int primaryGen) {
        luceneServerPrimary.getBlockingStub().stopIndex(StopIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
        luceneServerPrimary.getBlockingStub().startIndex(StartIndexRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setMode(Mode.PRIMARY)
                .setPrimaryGen(primaryGen)
                .build());
    }


    private void publishNRTAndValidateSearchResults(int numDocs) {
        // publish new NRT point (retrieve the current searcher version on primary)
        SearcherVersion searcherVersionPrimary = replicationServerPrimary.getReplicationServerBlockingStub().writeNRTPoint(
                IndexName.newBuilder().setIndexName(TEST_INDEX).build());
        assertEquals(true, searcherVersionPrimary.getDidRefresh());
        // primary should show numDocs hits now
        SearchResponse searchResponsePrimary = luceneServerPrimary.getBlockingStub().search(SearchRequest.newBuilder()
                .setIndexName(luceneServerPrimary.getTestIndex())
                .setStartHit(0)
                .setTopHits(10)
                .setVersion(searcherVersionPrimary.getVersion())
                .addAllRetrieveFields(RETRIEVED_VALUES)
                .build());
        // replica should also have numDocs hits
        SearchResponse searchResponseSecondary = luceneServerSecondary.getBlockingStub().search(SearchRequest.newBuilder()
                .setIndexName(luceneServerSecondary.getTestIndex())
                .setStartHit(0)
                .setTopHits(10)
                .setVersion(searcherVersionPrimary.getVersion())
                .addAllRetrieveFields(RETRIEVED_VALUES)
                .build());

        validateSearchResults(numDocs, searchResponsePrimary.getResponse());
        validateSearchResults(numDocs, searchResponseSecondary.getResponse());
    }

    public static void validateSearchResults(int numHitsExpected, String searchResponse) {
        Map<String, Object> resultMap = new Gson().fromJson(searchResponse, Map.class);
        assertEquals(numHitsExpected, (double) resultMap.get("totalHits"), 0.01);
        List<Map<String, Object>> hits = (List<Map<String, Object>>) resultMap.get("hits");
        assertEquals(numHitsExpected, ((List<Map<String, Object>>) resultMap.get("hits")).size());
        Map<String, Object> firstHit = hits.get(0);
        checkHits(firstHit);
        Map<String, Object> secondHit = hits.get(1);
        checkHits(secondHit);
    }


}
