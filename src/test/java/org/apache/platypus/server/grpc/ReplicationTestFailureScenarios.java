package org.apache.platypus.server.grpc;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.gson.Gson;
import io.findify.s3mock.S3Mock;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.platypus.server.luceneserver.GlobalState;
import org.apache.platypus.server.utils.Archiver;
import org.apache.platypus.server.utils.ArchiverImpl;
import org.apache.platypus.server.utils.TarImpl;
import org.apache.platypus.server.luceneserver.ShardState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

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

    private Path primaryStateDir;
    private GrpcServer luceneServerPrimary;
    private GrpcServer replicationServerPrimary;

    private GrpcServer luceneServerSecondary;
    private GrpcServer replicationServerSecondary;

    private final String BUCKET_NAME = "archiver-unittest";
    private Archiver archiver;
    private S3Mock api;
    private AmazonS3 s3;
    private Path s3Directory;
    private Path archiverDirectory;

    @After
    public void tearDown() throws IOException {
        api.shutdown();
        luceneServerPrimary.getGlobalState().close();
        luceneServerSecondary.getGlobalState().close();
        rmDir(Paths.get(luceneServerPrimary.getRootDirName()));
        rmDir(Paths.get(luceneServerSecondary.getRootDirName()));
    }

    @Before
    public void setUp() throws IOException {
        //setup S3 for backup/restore
        s3Directory = folder.newFolder("s3").toPath();
        archiverDirectory = folder.newFolder("archiver").toPath();
        api = S3Mock.create(8011, s3Directory.toAbsolutePath().toString());
        api.start();
        s3 = new AmazonS3Client(new AnonymousAWSCredentials());
        s3.setEndpoint("http://127.0.0.1:8011");
        s3.createBucket(BUCKET_NAME);
        archiver = new ArchiverImpl(s3, BUCKET_NAME, archiverDirectory, new TarImpl());

        startPrimaryServer(null);
        startSecondaryServer();
    }

    public void startPrimaryServer(Path stateDir) throws IOException {
        //set up primary servers
        String nodeNamePrimary = "serverPrimary";
        String rootDirNamePrimary = "serverPrimaryRootDirName";
        rmDir(Paths.get(folder.getRoot().toString(), rootDirNamePrimary));
        String testIndex = TEST_INDEX;
        GlobalState globalStatePrimary;
        if (stateDir == null) {
            Path rootDirPrimary = folder.newFolder(rootDirNamePrimary).toPath();
            globalStatePrimary = new GlobalState(nodeNamePrimary, rootDirPrimary, "localhost", 9900, 9001);
            this.primaryStateDir = rootDirPrimary;
        } else {
            globalStatePrimary = new GlobalState(nodeNamePrimary, stateDir, "localhost", 9900, 9001);
            this.primaryStateDir = stateDir;
        }
        luceneServerPrimary = new GrpcServer(grpcCleanup, folder, false, globalStatePrimary, nodeNamePrimary, testIndex, globalStatePrimary.getPort(), archiver);
        replicationServerPrimary = new GrpcServer(grpcCleanup, folder, true, globalStatePrimary, nodeNamePrimary, testIndex, 9001, archiver);

    }

    public void startSecondaryServer() throws IOException {
        //set up secondary servers
        String nodeNameSecondary = "serverSecondary";
        String rootDirNameSecondary = "serverSecondaryRootDirName";
        rmDir(Paths.get(folder.getRoot().toString(), rootDirNameSecondary));
        Path rootDirSecondary = folder.newFolder(rootDirNameSecondary).toPath();
        String testIndex = TEST_INDEX;
        GlobalState globalStateSecondary = new GlobalState(nodeNameSecondary, rootDirSecondary, "localhost", 9902, 9003);
        luceneServerSecondary = new GrpcServer(grpcCleanup, folder, false, globalStateSecondary, nodeNameSecondary, testIndex, globalStateSecondary.getPort(), archiver);
        replicationServerSecondary = new GrpcServer(grpcCleanup, folder, true, globalStateSecondary, nodeNameSecondary, testIndex, 9003, archiver);
    }

    public void shutdownPrimaryServer() throws IOException {
        luceneServerPrimary.getGlobalState().close();
        rmDir(Paths.get(luceneServerPrimary.getRootDirName()));
        rmDir(primaryStateDir);
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
    public void primaryStoppedAndRestartedWithNoPreviousIndex() throws IOException, InterruptedException {
        //startIndex Primary
        GrpcServer.TestServer testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
        //startIndex replica
        GrpcServer.TestServer testServerReplica = new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
        //add 2 docs to primary
        testServerPrimary.addDocuments();
        //add 2 docs to primary
        testServerPrimary.addDocuments();
        //both primary and replica should have 4 docs
        publishNRTAndValidateSearchResults(4);
        //backup primary
        backupIndex();
        //commit secondary state
        luceneServerSecondary.getBlockingStub().commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());

        //non-graceful primary shutdown (i.e. blow away index directory)
        shutdownPrimaryServer();
        //start primary again with new empty index directory
        startPrimaryServer(null);
        //startIndex Primary with primaryGen = 1
        testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY, 1);
        //stop and restart secondary to connect to "new" primary
        gracefullRestartSecondary();
        //add 2 docs to primary
        testServerPrimary.addDocuments();
        //both primary and secondary have only 2 docs now (replica has fewer docs than before as expected)
        publishNRTAndValidateSearchResults(2);

    }

    @Test
    public void primaryStoppedAndRestartedWithPreviousLocalIndex() throws IOException, InterruptedException {
        //startIndex primary
        GrpcServer.TestServer testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
        //startIndex replica
        GrpcServer.TestServer testServerReplica = new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
        //add 2 docs to primary
        testServerPrimary.addDocuments();
        //both primary and replica should have 2 docs
        publishNRTAndValidateSearchResults(2);
        //commit primary
        luceneServerPrimary.getBlockingStub().commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());
        //commit metadata state for secondary
        luceneServerSecondary.getBlockingStub().commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());

        //gracefully stop and restart primary
        gracefullRestartPrimary(1);
        //stop and restart secondary to connect to "new" primary
        gracefullRestartSecondary();
        //add 2 docs to primary
        testServerPrimary.addDocuments();
        publishNRTAndValidateSearchResults(4);

    }

    @Test
    public void primaryDurabilityBasic() throws IOException, InterruptedException {
        //startIndex primary
        GrpcServer.TestServer testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
        //add 2 docs to primary
        testServerPrimary.addDocuments();
        //backup index
        backupIndex();
        //non-graceful primary shutdown (i.e. blow away index and state directory)
        shutdownPrimaryServer();
        //start primary again with latest commit point
        startPrimaryServer(null);
        //startIndex Primary with primaryGen = 1
        testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY, 1, true);
        //add 2 docs to primary
        testServerPrimary.addDocuments();

        // publish new NRT point (retrieve the current searcher version on primary)
        SearcherVersion searcherVersionPrimary = replicationServerPrimary.getReplicationServerBlockingStub().writeNRTPoint(
                IndexName.newBuilder().setIndexName(TEST_INDEX).build());

        // primary should show numDocs hits now
        SearchResponse searchResponsePrimary = luceneServerPrimary.getBlockingStub().search(SearchRequest.newBuilder()
                .setIndexName(luceneServerPrimary.getTestIndex())
                .setStartHit(0)
                .setTopHits(10)
                .setVersion(searcherVersionPrimary.getVersion())
                .addAllRetrieveFields(RETRIEVED_VALUES)
                .build());

        validateSearchResults(4, searchResponsePrimary);
    }

    @Test
    public void primaryDurabilitySyncWithReplica() throws IOException, InterruptedException {
        //startIndex primary
        GrpcServer.TestServer testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
        //startIndex replica
        GrpcServer.TestServer testServerReplica = new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
        //add 2 docs to primary
        testServerPrimary.addDocuments();
        //both primary and replica should have 2 docs
        publishNRTAndValidateSearchResults(2);

        //backupIndex (with 2 docs)
        backupIndex();
        //commit metadata state for secondary, since we wil reuse this upon startIndex
        luceneServerSecondary.getBlockingStub().commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());

        //add 6 more docs to primary but dont commit, NRT is at 8 docs but commit point at 2 docs
        testServerPrimary.addDocuments();
        testServerPrimary.addDocuments();
        testServerPrimary.addDocuments();
        publishNRTAndValidateSearchResults(8);

        //non-graceful primary shutdown (i.e. blow away index directory and stateDir)
        shutdownPrimaryServer();

        //start primary again, download data from latest commit point
        startPrimaryServer(null);
        //startIndex Primary with primaryGen = 1
        testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY, 1, true);
        //stop and restart secondary to connect to "new" primary,
        gracefullRestartSecondary();
        //add 2 more docs to primary
        testServerPrimary.addDocuments();
        publishNRTAndValidateSearchResults(4);

    }

    @Test
    public void primaryDurabilityWithMultipleCommits() throws IOException, InterruptedException {
        //startIndex primary
        GrpcServer.TestServer testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
        for (int i = 0; i < 5; i++) {
            //add 4 docs to primary
            testServerPrimary.addDocuments();
            testServerPrimary.addDocuments();
            backupIndex();
        }

        //non-graceful primary shutdown (i.e. blow away index directory and stateDir)
        shutdownPrimaryServer();

        //start primary again, download data from latest commit point
        startPrimaryServer(null);
        //startIndex Primary with primaryGen = 1
        testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY, 1, true);

        //add 2 docs to primary
        testServerPrimary.addDocuments();

        // publish new NRT point (retrieve the current searcher version on primary)
        SearcherVersion searcherVersionPrimary = replicationServerPrimary.getReplicationServerBlockingStub().writeNRTPoint(
                IndexName.newBuilder().setIndexName(TEST_INDEX).build());

        // primary should show numDocs hits now
        SearchResponse searchResponsePrimary = luceneServerPrimary.getBlockingStub().search(SearchRequest.newBuilder()
                .setIndexName(luceneServerPrimary.getTestIndex())
                .setStartHit(0)
                .setTopHits(22)
                .setVersion(searcherVersionPrimary.getVersion())
                .addAllRetrieveFields(RETRIEVED_VALUES)
                .build());

        validateSearchResults(22, searchResponsePrimary);
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

        validateSearchResults(numDocs, searchResponsePrimary);
        validateSearchResults(numDocs, searchResponseSecondary);
    }

    public static void validateSearchResults(int numHitsExpected, SearchResponse searchResponse) {
        assertEquals(numHitsExpected, searchResponse.getTotalHits());
        assertEquals(numHitsExpected, searchResponse.getHitsList().size());
        SearchResponse.Hit firstHit = searchResponse.getHits(0);
        checkHits(firstHit);
        SearchResponse.Hit secondHit = searchResponse.getHits(1);
        checkHits(secondHit);
    }

    private void backupIndex() {
        luceneServerPrimary.getBlockingStub().backupIndex(BackupIndexRequest.newBuilder()
                .setIndexName("test_index")
                .setServiceName("testservice")
                .setResourceName("testresource")
                .build()
        );


    }
}
