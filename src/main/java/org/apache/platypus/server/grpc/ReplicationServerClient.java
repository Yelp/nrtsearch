package org.apache.platypus.server.grpc;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ReplicationServerClient implements Closeable {
    public final static int BINARY_MAGIC = 0x3414f5c;
    Logger logger = LoggerFactory.getLogger(ReplicationServerClient.class);
    private final String host;
    private final int port;
    private final ManagedChannel channel;
    private final ReplicationServerGrpc.ReplicationServerBlockingStub blockingStub;
    private final ReplicationServerGrpc.ReplicationServerStub asyncStub;

    /**
     * Construct client connecting to ReplicationServer server at {@code host:port}.
     */
    public ReplicationServerClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build(), host, port);
    }

    /**
     * Construct client for accessing ReplicationServer server using the existing channel.
     */
    ReplicationServerClient(ManagedChannel channel, String host, int port) {
        this.channel = channel;
        blockingStub = ReplicationServerGrpc.newBlockingStub(channel);
        asyncStub = ReplicationServerGrpc.newStub(channel);
        this.host = host;
        this.port = port;
    }


    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public void close() throws IOException {
        try {
            shutdown();
        } catch (InterruptedException e) {
            logger.warn("channel shutdown interrupted.", e);
            new RuntimeException(e);
        }
    }

    private void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public AddReplicaResponse addReplicas(String indexName, int replicaId, String hostName, int port) {
        AddReplicaRequest addDocumentRequest = AddReplicaRequest.newBuilder()
                .setMagicNumber(BINARY_MAGIC)
                .setIndexName(indexName)
                .setReplicaId(replicaId)
                .setHostName(hostName)
                .setPort(port)
                .build();
        return this.blockingStub.addReplicas(addDocumentRequest);
    }

    public Iterator<RawFileChunk> recvRawFile(String fileName, long fpOffset) {
        FileInfo fileInfo = FileInfo.newBuilder().setFileName(fileName).setFpStart(fpOffset).build();
        return this.blockingStub.recvRawFile(fileInfo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicationServerClient that = (ReplicationServerClient) o;
        return port == that.port &&
                Objects.equals(logger, that.logger) &&
                Objects.equals(host, that.host) &&
                Objects.equals(channel, that.channel) &&
                Objects.equals(blockingStub, that.blockingStub) &&
                Objects.equals(asyncStub, that.asyncStub);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logger, host, port, channel, blockingStub, asyncStub);
    }

    public static class ReplicationServerClientManager<T extends GeneratedMessageV3> {
        Logger logger = LoggerFactory.getLogger(ReplicationServerClientManager.class);
        private static final String LOCALHOST = "localhost";
        private static final int PORT = 50052;
        Map<RequestType, ReplicationServerClient> replicationServerClients = new ConcurrentHashMap<>();

        public enum RequestType {
            ADD_REPLICAS,
            COPY_FILES,
            WRT_NRT_POINT,
            NEW_NRT_POINT,
            SEND_ME_FILES;
        }

        /* Each requestType reuses its channel (connection), so we need to make sure only one
         * instance of ReplicationServerClientManager is used by callers */
        @SuppressWarnings("unchecked")
        public T sendRequest(RequestType requestType, T request) {
            ReplicationServerClient client;
            switch (requestType) {
                case ADD_REPLICAS:
                    client = replicationServerClients.computeIfAbsent(RequestType.ADD_REPLICAS, c -> new ReplicationServerClient(LOCALHOST, PORT));
                    AddReplicaRequest addReplicaRequest = (AddReplicaRequest) request;
                    AddReplicaResponse response = client.blockingStub.addReplicas(addReplicaRequest);
                    return (T) response;
                case COPY_FILES:
                    client = replicationServerClients.computeIfAbsent(RequestType.COPY_FILES, c -> new ReplicationServerClient(LOCALHOST, PORT));
                    break;
                case WRT_NRT_POINT:
                    client = replicationServerClients.computeIfAbsent(RequestType.WRT_NRT_POINT, c -> new ReplicationServerClient(LOCALHOST, PORT));
                    break;
                case NEW_NRT_POINT:
                    client = replicationServerClients.computeIfAbsent(RequestType.NEW_NRT_POINT, c -> new ReplicationServerClient(LOCALHOST, PORT));
                    break;
                case SEND_ME_FILES:
                    client = replicationServerClients.computeIfAbsent(RequestType.SEND_ME_FILES, c -> new ReplicationServerClient(LOCALHOST, PORT));
                    break;
                default:
                    logger.info(String.format(String.format("Invalid request type %s Supported requestTypes: %s", requestType, RequestType.values())));
                    break;
            }
            return null;
        }
    }

}
