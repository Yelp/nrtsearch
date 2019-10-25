package org.apache.platypus.server.grpc;

import io.grpc.testing.GrpcCleanupRule;
import org.apache.platypus.server.GlobalState;
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

import static org.apache.platypus.server.grpc.GrpcChannel.rmDir;
import static org.junit.Assert.assertEquals;

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

    private GrpcChannel startLuenceChannelPrimary;
    private GrpcChannel startReplicationChannelPrimary;

    private GrpcChannel startLuenceChannelSecondary;
    private GrpcChannel startReplicationChannelSecondary;

    @After
    public void tearDown() throws IOException {
        startLuenceChannelPrimary.getGlobalState().close();
        startLuenceChannelSecondary.getGlobalState().close();
        rmDir(Paths.get(startLuenceChannelPrimary.getRootDirName()));
        rmDir(Paths.get(startLuenceChannelSecondary.getRootDirName()));
    }

    @Before
    public void setUp() throws IOException {
        //set up primary channels
        String nodeNamePrimary = "serverPrimary";
        String rootDirNamePrimary = "serverPrimaryRootDirName";
        Path rootDirPrimary = folder.newFolder(rootDirNamePrimary).toPath();
        String testIndex = "test_index";
        GlobalState globalStatePrimary = new GlobalState(nodeNamePrimary, rootDirPrimary, "localhost", 9900);
        startReplicationChannelPrimary = new GrpcChannel(grpcCleanup, folder, true, globalStatePrimary, nodeNamePrimary, testIndex);
        startLuenceChannelPrimary = new GrpcChannel(grpcCleanup, folder, false, globalStatePrimary, nodeNamePrimary, testIndex);

        //set up secondary channels
        String nodeNameSecondary = "serverSecondary";
        String rootDirNameSecondary = "serverSecondaryRootDirName";
        Path rootDirSecondary = folder.newFolder(rootDirNameSecondary).toPath();
        GlobalState globalStateSecondary = new GlobalState(nodeNameSecondary, rootDirSecondary, "localhost", 9900);
        startReplicationChannelSecondary = new GrpcChannel(grpcCleanup, folder, true, globalStateSecondary, nodeNameSecondary, testIndex);
        startLuenceChannelSecondary = new GrpcChannel(grpcCleanup, folder, false, globalStateSecondary, nodeNameSecondary, testIndex);

    }

    @Test
    public void recvCopyState() throws IOException, InterruptedException {
        GrpcChannel.TestServer testServer = new GrpcChannel.TestServer(startLuenceChannelPrimary, true, Mode.PRIMARY);
        testServer.addDocuments();
        assertEquals(false, testServer.error);
        assertEquals(true, testServer.completed);

        //This causes the copyState on primary to be refreshed
        startLuenceChannelPrimary.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName("test_index").build());

        CopyStateRequest copyStateRequest = CopyStateRequest.newBuilder()
                .setMagicNumber(ReplicationServerClient.BINARY_MAGIC)
                .setIndexName(startReplicationChannelPrimary.getTestIndex())
                .setReplicaId(0).build();
        CopyState copyState = startReplicationChannelPrimary.getReplicationServerBlockingStub().recvCopyState(copyStateRequest);
        assertEquals(0, copyState.getGen());
        FilesMetadata filesMetadata = copyState.getFilesMetadata();
        assertEquals(3, filesMetadata.getNumFiles());
    }

    @Test
    public void copyFiles() throws IOException, InterruptedException {
        GrpcChannel.TestServer testServerPrimary = new GrpcChannel.TestServer(startLuenceChannelPrimary, true, Mode.PRIMARY);
        testServerPrimary.addDocuments();

        GrpcChannel.TestServer testServerReplica = new GrpcChannel.TestServer(startLuenceChannelSecondary, true, Mode.REPLICA);
    }

    @Test
    public void addReplica() {

    }
}
