/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.platypus.server;

import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.IndexReader;
import org.apache.platypus.server.grpc.Mode;
import org.apache.platypus.server.grpc.ReplicationServerClient;
import org.apache.platypus.server.grpc.StartIndexRequest;
import org.apache.platypus.server.grpc.StartIndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StartIndexHandler implements Handler<StartIndexRequest, StartIndexResponse> {
    Logger logger = LoggerFactory.getLogger(StartIndexHandler.class);

    @Override
    public StartIndexResponse handle(IndexState indexState, StartIndexRequest startIndexRequest) throws StartIndexHandlerException {
        final ShardState shardState = indexState.getShard(0);
        final Mode mode = startIndexRequest.getMode();
        final long primaryGen;
        final String primaryAddress;
        final int primaryPort;
        if (mode.equals(Mode.PRIMARY)) {
            primaryGen = startIndexRequest.getPrimaryGen();
            primaryAddress = null;
            primaryPort = -1;
        } else if (mode.equals(Mode.REPLICA)) {
            primaryGen = startIndexRequest.getPrimaryGen();
            primaryAddress = startIndexRequest.getPrimaryAddress();
            primaryPort = startIndexRequest.getPort();
        } else {
            primaryGen = -1;
            primaryAddress = null;
            primaryPort = -1;
        }

        long t0 = System.nanoTime();
        try {
            if (mode.equals(Mode.PRIMARY)) {
                shardState.startPrimary(primaryGen);
            } else if (mode.equals(Mode.REPLICA)) {
                // channel/client to talk to primary on
                ReplicationServerClient primaryNodeClient = new ReplicationServerClient(primaryAddress, primaryPort);
                shardState.startReplica(primaryNodeClient, primaryGen);
            } else {
                indexState.start();
            }
        } catch (Exception e) {
            logger.error("Cannot start IndexState/ShardState", e);
            throw new StartIndexHandlerException(e);
        }

        StartIndexResponse.Builder startIndexResponseBuilder = StartIndexResponse.newBuilder();
        SearcherTaxonomyManager.SearcherAndTaxonomy s;
        try {
            s = shardState.acquire();
        } catch (IOException e) {
            logger.error("Acquire shard state failed", e);
            throw new StartIndexHandlerException(e);
        }
        try {
            IndexReader r = s.searcher.getIndexReader();
            startIndexResponseBuilder.setMaxDoc(r.maxDoc());
            startIndexResponseBuilder.setNumDocs(r.numDocs());
            startIndexResponseBuilder.setSegments(r.toString());
        } finally {
            try {
                shardState.release(s);
            } catch (IOException e) {
                logger.error("Release shard state failed", e);
                throw new StartIndexHandlerException(e);
            }
        }
        long t1 = System.nanoTime();
        startIndexResponseBuilder.setStartTimeMS(((t1 - t0) / 1000000.0));
        return startIndexResponseBuilder.build();
    }

    public static class StartIndexHandlerException extends HandlerException {

        public StartIndexHandlerException(Exception e) {
                super(e);
        }
    }
}
