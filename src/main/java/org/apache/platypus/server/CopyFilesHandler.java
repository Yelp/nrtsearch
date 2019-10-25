package org.apache.platypus.server;

import io.grpc.stub.StreamObserver;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.platypus.server.grpc.CopyFiles;
import org.apache.platypus.server.grpc.TransferStatus;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CopyFilesHandler implements Handler<CopyFiles, TransferStatus> {


    public TransferStatus handle(IndexState indexState, CopyFiles copyFilesRequest) throws HandlerException {
        String indexName = copyFilesRequest.getIndexName();
        ShardState shardState = indexState.getShard(0);

        if (shardState.isReplica() == false) {
            throw new IllegalArgumentException("index \"" + indexName + "\" is not a replica or was not started yet");
        }

        if (!isValidMagicHeader(copyFilesRequest.getMagicNumber())) {
            throw new RuntimeException("RecvCopyStateHandler invoked with Invalid Magic Number");
        }

        long primaryGen = copyFilesRequest.getPrimaryGen();

        // these are the files that the remote (primary) wants us to copy
        Map<String, FileMetaData> files = null;
        try {
            files = NRTReplicaNode.readFilesMetaData(copyFilesRequest.getFilesMetadata());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        AtomicBoolean finished = new AtomicBoolean();
        try {
            CopyJob job = shardState.nrtReplicaNode.launchPreCopyFiles(finished, primaryGen, files);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // we hold open this request, only finishing/closing once our copy has finished, so primary knows when we finished
        while (true) {
            // nocommit don't poll!  use a condition...
            if (finished.get()) {
                break;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // TODO: keep alive mechanism so primary can better "guess" when we dropped off
        }

//        out.writeByte((byte) 1);
//        streamOut.flush();
        return null;
    }
}
