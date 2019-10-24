package org.apache.platypus.server;

import org.apache.platypus.server.grpc.AddReplicaRequest;
import org.apache.platypus.server.grpc.AddReplicaResponse;
import org.apache.platypus.server.grpc.ReplicationServerClient;

import java.io.IOException;
import java.net.InetSocketAddress;

public class AddReplicaHandler implements Handler<AddReplicaRequest, AddReplicaResponse> {
    @Override
    public AddReplicaResponse handle(IndexState indexState, AddReplicaRequest addReplicaRequest) {
        ShardState shardState = indexState.getShard(0);
        if (shardState.isPrimary() == false) {
            throw new IllegalArgumentException("index \"" + indexState.name + "\" was not started or is not a primary");
        }

        if(!isValidMagicHeader(addReplicaRequest.getMagicNumber())){
            throw new RuntimeException("AddReplica invoked with Invalid Magic Number");
        }
        try {
            shardState.nrtPrimaryNode.addReplica(addReplicaRequest.getReplicaId(),
                    new ReplicationServerClient(addReplicaRequest.getHostName(), addReplicaRequest.getPort()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return AddReplicaResponse.newBuilder().setOk("ok").build();
    }
}
