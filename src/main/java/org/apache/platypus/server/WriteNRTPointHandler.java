package org.apache.platypus.server;

import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.platypus.server.grpc.IndexName;
import org.apache.platypus.server.grpc.ReplicationServerClient;
import org.apache.platypus.server.grpc.SearcherVersion;

import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;

public class WriteNRTPointHandler implements Handler<IndexName, SearcherVersion> {
    @Override
    public SearcherVersion handle(IndexState indexState, IndexName protoRequest) throws HandlerException {
        final ShardState shardState = indexState.getShard(0);

        if (shardState.isPrimary() == false) {
            throw new IllegalArgumentException("index \"" + shardState.name + "\" is either not started or is not a primary index");
        }
        SearcherVersion.Builder searchverVersionBuilder = SearcherVersion.newBuilder();
        try {
            if (shardState.nrtPrimaryNode.flushAndRefresh()) {
                // Something did get flushed (there were indexing ops since the last flush):

                // nocommit: we used to notify caller of the version, before trying to push to replicas, in case we crash after flushing but
                // before notifying all replicas, at which point we have a newer version index than client knew about?
                long version = shardState.nrtPrimaryNode.getCopyStateVersion();
                Queue<NRTPrimaryNode.ReplicaDetails> replicasInfos = shardState.nrtPrimaryNode.replicasInfos;
                shardState.nrtPrimaryNode.message("send flushed version=" + version + " replica count " + replicasInfos.size());

                // Notify current replicas:
                Iterator<NRTPrimaryNode.ReplicaDetails> it = replicasInfos.iterator();
                while (it.hasNext()) {
                    NRTPrimaryNode.ReplicaDetails replicaDetails = it.next();
                    int replicaID = replicaDetails.getReplicaId();
                    ReplicationServerClient currentReplicaServerClient = replicaDetails.getReplicationServerClient();
                    currentReplicaServerClient.newNRTPoint(indexState.name, shardState.nrtPrimaryNode.getPrimaryGen(), version);
                }
                return searchverVersionBuilder.setVersion(version).setDidRefresh(true).build();
            } else {
                SearcherTaxonomyManager.SearcherAndTaxonomy s = shardState.acquire();
                try {
                    long version = ((DirectoryReader) s.searcher.getIndexReader()).getVersion();
                    return searchverVersionBuilder.setVersion(version).setDidRefresh(false).build();
                } finally {
                    shardState.release(s);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
