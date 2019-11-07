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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.platypus.server.grpc.GrpcServer.rmDir;
import static org.apache.platypus.server.grpc.LuceneServerTest.RETRIEVED_VALUES;
import static org.apache.platypus.server.grpc.LuceneServerTest.checkHits;
import static org.apache.platypus.server.grpc.ReplicationServerClient.BINARY_MAGIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class ReplicationServerTest {
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
        //set up primary servers
        String nodeNamePrimary = "serverPrimary";
        String rootDirNamePrimary = "serverPrimaryRootDirName";
        Path rootDirPrimary = folder.newFolder(rootDirNamePrimary).toPath();
        String testIndex = "test_index";
        GlobalState globalStatePrimary = new GlobalState(nodeNamePrimary, rootDirPrimary, "localhost", 9900, 9001);
        luceneServerPrimary = new GrpcServer(grpcCleanup, folder, false, globalStatePrimary, nodeNamePrimary, testIndex, globalStatePrimary.getPort());
        replicationServerPrimary = new GrpcServer(grpcCleanup, folder, true, globalStatePrimary, nodeNamePrimary, testIndex, 9001);

        //set up secondary servers
        String nodeNameSecondary = "serverSecondary";
        String rootDirNameSecondary = "serverSecondaryRootDirName";
        Path rootDirSecondary = folder.newFolder(rootDirNameSecondary).toPath();
        GlobalState globalStateSecondary = new GlobalState(nodeNameSecondary, rootDirSecondary, "localhost", 9902, 9003);
        luceneServerSecondary = new GrpcServer(grpcCleanup, folder, false, globalStateSecondary, nodeNameSecondary, testIndex, globalStateSecondary.getPort());
        replicationServerSecondary = new GrpcServer(grpcCleanup, folder, true, globalStateSecondary, nodeNameSecondary, testIndex, 9003);

    }

    @Test
    public void recvCopyState() throws IOException, InterruptedException {
        GrpcServer.TestServer testServer = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
        testServer.addDocuments();
        assertEquals(false, testServer.error);
        assertEquals(true, testServer.completed);

        //This causes the copyState on primary to be refreshed
        luceneServerPrimary.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName("test_index").build());

        CopyStateRequest copyStateRequest = CopyStateRequest.newBuilder()
                .setMagicNumber(BINARY_MAGIC)
                .setIndexName(replicationServerPrimary.getTestIndex())
                .setReplicaId(0).build();
        CopyState copyState = replicationServerPrimary.getReplicationServerBlockingStub().recvCopyState(copyStateRequest);
        assertEquals(0, copyState.getGen());
        FilesMetadata filesMetadata = copyState.getFilesMetadata();
        assertEquals(3, filesMetadata.getNumFiles());
    }

    @Test
    public void copyFiles() throws IOException, InterruptedException {
        GrpcServer.TestServer testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
        testServerPrimary.addDocuments();

        //This causes the copyState on primary to be refreshed
        luceneServerPrimary.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName("test_index").build());

        //capture the copy state on primary (client node in this test case)
        CopyStateRequest copyStateRequest = CopyStateRequest.newBuilder()
                .setMagicNumber(BINARY_MAGIC)
                .setIndexName(replicationServerPrimary.getTestIndex())
                .setReplicaId(0).build();
        CopyState copyState = replicationServerPrimary.getReplicationServerBlockingStub().recvCopyState(copyStateRequest);
        assertEquals(0, copyState.getGen());
        FilesMetadata filesMetadata = copyState.getFilesMetadata();
        assertEquals(3, filesMetadata.getNumFiles());

        //send the file metadata info to replica
        GrpcServer.TestServer testServerReplica = new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
        CopyFiles.Builder requestBuilder = CopyFiles.newBuilder()
                .setMagicNumber(BINARY_MAGIC)
                .setIndexName("test_index")
                .setPrimaryGen(0);
        requestBuilder.setFilesMetadata(filesMetadata);

        Iterator<TransferStatus> transferStatusIterator = replicationServerSecondary.getReplicationServerBlockingStub().copyFiles(requestBuilder.build());
        int done = 0;
        int failed = 0;
        int ongoing = 0;
        while (transferStatusIterator.hasNext()) {
            TransferStatus transferStatus = transferStatusIterator.next();
            if (transferStatus.getCode().equals(TransferStatusCode.Done)) {
                done++;
            } else if (transferStatus.getCode().equals(TransferStatusCode.Failed)) {
                failed++;
            } else if (transferStatus.getCode().equals(TransferStatusCode.Ongoing)) {
                ongoing++;
            }
        }
        assertEquals(1, done);
        assertTrue(0 < ongoing);
        assertEquals(0, failed);
    }

    @Test
    public void addReplica() {

    }

    @Test
    public void basicReplication() throws IOException, InterruptedException {
        //index 2 documents to primary
        GrpcServer.TestServer testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
        testServerPrimary.addDocuments();
        //refresh (also sends NRTPoint to replicas, but none started at this point)
        luceneServerPrimary.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName("test_index").build());
        //startIndex replica
        GrpcServer.TestServer testServerReplica = new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
        //add 2 more docs to primary
        testServerPrimary.addDocuments();

        // publish new NRT point (retrieve the current searcher version on primary)
        SearcherVersion searcherVersionPrimary = replicationServerPrimary.getReplicationServerBlockingStub().writeNRTPoint(IndexName.newBuilder().setIndexName("test_index").build());
        assertEquals(true, searcherVersionPrimary.getDidRefresh());

        // primary should show 4 hits now
        SearchResponse searchResponsePrimary = luceneServerPrimary.getBlockingStub().search(SearchRequest.newBuilder()
                .setIndexName(luceneServerPrimary.getTestIndex())
                .setStartHit(0)
                .setTopHits(10)
                .setVersion(searcherVersionPrimary.getVersion())
                .addAllRetrieveFields(RETRIEVED_VALUES)
                .build());

        // replica should too!
        SearchResponse searchResponseSecondary = luceneServerSecondary.getBlockingStub().search(SearchRequest.newBuilder()
                .setIndexName(luceneServerSecondary.getTestIndex())
                .setStartHit(0)
                .setTopHits(10)
                .setVersion(searcherVersionPrimary.getVersion())
                .addAllRetrieveFields(RETRIEVED_VALUES)
                .build());

        searchResponseSecondary.getResponse();

        validateSearchResults(searchResponsePrimary.getResponse());
        validateSearchResults(searchResponseSecondary.getResponse());

    }

    public static void validateSearchResults(String searchResponse) {
        Map<String, Object> resultMap = new Gson().fromJson(searchResponse, Map.class);
        assertEquals(4.0, (double) resultMap.get("totalHits"), 0.01);
        List<Map<String, Object>> hits = (List<Map<String, Object>>) resultMap.get("hits");
        assertEquals(4, ((List<Map<String, Object>>) resultMap.get("hits")).size());
        Map<String, Object> firstHit = hits.get(0);
        checkHits(firstHit);
        Map<String, Object> secondHit = hits.get(1);
        checkHits(secondHit);

    }


}
