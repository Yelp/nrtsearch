package org.apache.platypus.server;

import org.apache.platypus.server.grpc.IndexName;
import org.apache.platypus.server.grpc.SearcherVersion;

import java.io.IOException;

public class ReplicaCurrentSearchingVersionHandler implements Handler<IndexName, SearcherVersion> {

    @Override
    public SearcherVersion handle(IndexState indexState, IndexName indexNameRequest) throws HandlerException {
        ShardState shardState = indexState.getShard(0);
        if (shardState.isReplica() == false) {
            throw new IllegalArgumentException("index \"" + indexNameRequest.getIndexName() + "\" is not a replica or was not started yet");
        }
        try {
            long currentSearchingVersion = shardState.nrtReplicaNode.getCurrentSearchingVersion();
            return SearcherVersion.newBuilder().setVersion(currentSearchingVersion).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

