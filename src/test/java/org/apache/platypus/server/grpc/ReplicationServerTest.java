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

import static org.apache.platypus.server.grpc.StartChannel.rmDir;
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

    private StartChannel startReplicationChannel;
    private StartChannel startLuenceChannel;

    @After
    public void tearDown() throws IOException {
        startLuenceChannel.getGlobalState().close();
        rmDir(Paths.get(startLuenceChannel.getRootDirName()));
    }

    @Before
    public void setUp() throws IOException {
        String nodeName = "server1";
        String rootDirName = "server1RootDirName1";
        Path rootDir = folder.newFolder(rootDirName).toPath();
        String testIndex = "test_index";
        GlobalState globalState = new GlobalState(nodeName, rootDir, "localhost", 9900);
        startReplicationChannel = new StartChannel(grpcCleanup, folder, true, globalState, nodeName, testIndex);
        startLuenceChannel = new StartChannel(grpcCleanup, folder, false, globalState, nodeName, testIndex);
    }

    @Test
    public void recvCopyState() throws IOException, InterruptedException {
        StartChannel.TestAddDocuments testAddDocs = new StartChannel.TestAddDocuments(startLuenceChannel, true, true);
        testAddDocs.addDocuments();
        assertEquals(false, testAddDocs.error);
        assertEquals(true, testAddDocs.completed);

        startLuenceChannel.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName("test_index").build());

        CopyStateRequest copyStateRequest = CopyStateRequest.newBuilder()
                .setMagicNumber(ReplicationServerClient.BINARY_MAGIC)
                .setIndexName(startReplicationChannel.getTestIndex())
                .setReplicaId(0).build();
        CopyState copyState = startReplicationChannel.getReplicationServerBlockingStub().recvCopyState(copyStateRequest);
        System.out.println(copyState);
    }

}
