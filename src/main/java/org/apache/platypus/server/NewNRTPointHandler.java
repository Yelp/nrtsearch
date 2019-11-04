package org.apache.platypus.server;

import org.apache.platypus.server.grpc.NewNRTPoint;
import org.apache.platypus.server.grpc.TransferStatus;
import org.apache.platypus.server.grpc.TransferStatusCode;

import java.io.IOException;

public class NewNRTPointHandler implements Handler<NewNRTPoint, TransferStatus> {

    @Override
    public TransferStatus handle(IndexState indexState, NewNRTPoint newNRTPointRequest) throws HandlerException {
        ShardState shardState = indexState.getShard(0);
        if (shardState.isReplica() == false) {
            throw new IllegalArgumentException("index \"" + newNRTPointRequest.getIndexName() + "\" is not a replica or was not started yet");
        }

        long version = newNRTPointRequest.getVersion();
        long newPrimaryGen = newNRTPointRequest.getPrimaryGen();
        try {
            shardState.nrtReplicaNode.newNRTPoint(newPrimaryGen, version);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return TransferStatus.newBuilder()
                .setMessage("Replica kicked off a job (runs in the background) to copy files across, and open a new reader once that's done.")
                .setCode(TransferStatusCode.Done)
                .build();
    }
}
