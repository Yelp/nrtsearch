package org.apache.platypus.server;

import com.google.protobuf.ByteString;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.platypus.server.grpc.CopyState;
import org.apache.platypus.server.grpc.CopyStateRequest;
import org.apache.platypus.server.grpc.FileMetadata;
import org.apache.platypus.server.grpc.FilesMetadata;

import java.io.IOException;
import java.util.Map;

public class RecvCopyStateHandler implements Handler<CopyStateRequest, CopyState> {
    @Override
    public CopyState handle(IndexState indexState, CopyStateRequest copyStateRequest) {
        ShardState shardState = indexState.getShard(0);
        if (shardState.isPrimary() == false) {
            throw new IllegalArgumentException("index \"" + indexState.name + "\" was not started or is not a primary");
        }

        if (!isValidMagicHeader(copyStateRequest.getMagicNumber())) {
            throw new RuntimeException("RecvCopyStateHandler invoked with Invalid Magic Number");
        }
        org.apache.lucene.replicator.nrt.CopyState copyState = null;
        try {
            // Caller does not have CopyState; we pull the latest NRT point:
            copyState = shardState.nrtPrimaryNode.getCopyState();
            return RecvCopyStateHandler.writeCopyState(copyState);
        } catch (IOException e) {
            shardState.nrtPrimaryNode.message("top: exception during fetch: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            if (copyState != null) {
                shardState.nrtPrimaryNode.message("top: fetch: now release CopyState");
                try {
                    shardState.nrtPrimaryNode.releaseCopyState(copyState);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static CopyState writeCopyState(org.apache.lucene.replicator.nrt.CopyState state) throws IOException {
        CopyState.Builder builder = CopyState.newBuilder();
        builder.setInfoBytesLength(state.infosBytes.length);
        builder.setInfoBytes(ByteString.copyFrom(state.infosBytes, 0, state.infosBytes.length));

        builder.setGen(state.gen);
        builder.setVersion(state.version);

        FilesMetadata filesMetadata = writeFilesMetaData(state.files);
        builder.setFilesMetadata(filesMetadata);

        builder.setCompletedMergeFilesSize(state.completedMergeFiles.size());

        for (String fileName : state.completedMergeFiles) {
            builder.addCompletedMergeFiles(fileName);
        }

        builder.setPrimaryGen(state.primaryGen);

        return builder.build();
    }

    private static FilesMetadata writeFilesMetaData(Map<String, FileMetaData> files) throws IOException {
        FilesMetadata.Builder builder = FilesMetadata.newBuilder();
        builder.setNumFiles(files.size());

        for (Map.Entry<String, FileMetaData> ent : files.entrySet()) {
            FileMetadata.Builder fileMetadataBuilder = FileMetadata.newBuilder();
            fileMetadataBuilder.setFileName(ent.getKey());

            FileMetaData fmd = ent.getValue();
            fileMetadataBuilder.setLen(fmd.length);
            fileMetadataBuilder.setChecksum(fmd.checksum);
            fileMetadataBuilder.setHeaderLength(fmd.header.length);
            fileMetadataBuilder.setHeader(ByteString.copyFrom(fmd.header, 0, fmd.header.length));
            fileMetadataBuilder.setFooterLength(fmd.footer.length);
            fileMetadataBuilder.setFooter(ByteString.copyFrom(fmd.footer, 0, fmd.footer.length));
            builder.addFileMetadata(fileMetadataBuilder.build());
        }
        return builder.build();
    }


}
