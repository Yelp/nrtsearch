package org.apache.platypus.server.grpc;

/*
 * Copyright 2015 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherLifetimeManager;
import org.apache.platypus.server.*;
import org.apache.platypus.server.config.LuceneServerConfiguration;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Server that manages startup/shutdown of a {@code LuceneServer} server.
 */
public class LuceneServer {
    private static final Logger logger = Logger.getLogger(LuceneServer.class.getName());

    private Server server;
    private LuceneServerConfiguration luceneServerConfiguration;

    public LuceneServer(LuceneServerConfiguration luceneServerConfiguration) {
        this.luceneServerConfiguration = luceneServerConfiguration;
    }

    private void start() throws IOException {
        GlobalState globalState = new GlobalState(luceneServerConfiguration.getNodeName(), luceneServerConfiguration.getStateDir());
        /* The port on which the server should run */
        server = ServerBuilder.forPort(luceneServerConfiguration.getPort())
                .addService(new LuceneServerImpl(globalState))
                .build()
                .start();
        logger.info("Server started, listening on " + luceneServerConfiguration.getPort());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                logger.severe("*** shutting down gRPC server since JVM is shutting down");
                LuceneServer.this.stop();
                logger.severe("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        //TODO parse the LuceneServerConfiguration from a yaml file to be able to customize it.
        LuceneServerConfiguration luceneServerConfiguration = new LuceneServerConfiguration.Builder().build();
        final LuceneServer server = new LuceneServer(luceneServerConfiguration);
        server.start();
        server.blockUntilShutdown();
    }

    static class LuceneServerImpl extends LuceneServerGrpc.LuceneServerImplBase {
        private final GlobalState globalState;

        LuceneServerImpl(GlobalState globalState) {
            this.globalState = globalState;
        }

        @Override
        public void createIndex(CreateIndexRequest req, StreamObserver<CreateIndexResponse> responseObserver) {
            IndexState indexState = null;
            try {
                //TODO validate indexName e.g only allow a-z, A-Z, 0-9
                indexState = globalState.createIndex(req.getIndexName(), Paths.get(req.getRootDir()));
                // Create the first shard
                logger.info("NOW ADD SHARD 0");
                indexState.addShard(0, true);
                logger.info("DONE ADD SHARD 0");
                String response = String.format("Created Index name: %s, at rootDir: %s", req.getIndexName(), req.getRootDir());
                CreateIndexResponse reply = CreateIndexResponse.newBuilder().setResponse(response).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            } catch (IllegalArgumentException e) {
                logger.log(Level.WARNING, "invalid IndexName: " + req.getIndexName(), e);
                responseObserver.onError(Status
                        .ALREADY_EXISTS
                        .withDescription("invalid indexName: " + req.getIndexName())
                        .augmentDescription("IllegalArgumentException()")
                        .withCause(e)
                        .asRuntimeException());
            } catch (Exception e) {
                logger.log(Level.WARNING, "error while trying to save index state to disk for indexName: " + req.getIndexName() + "at rootDir: " + req.getRootDir() + req.getIndexName(), e);
                responseObserver.onError(Status
                        .INTERNAL
                        .withDescription("error while trying to save index state to disk for indexName: " + req.getIndexName() + "at rootDir: " + req.getRootDir())
                        .augmentDescription(e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
            }
        }

        @Override
        public void liveSettings(LiveSettingsRequest req, StreamObserver<LiveSettingsResponse> responseObserver) {
            try {
                IndexState indexState = globalState.getIndex(req.getIndexName());
                LiveSettingsResponse reply = new LiveSettingsHandler().handle(indexState, req);
                logger.info("LiveSettingsHandler returned " + reply.toString());
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            } catch (IllegalArgumentException e) {
                logger.log(Level.WARNING, "index: " + req.getIndexName() + " was not yet created", e);
                responseObserver.onError(Status
                        .ALREADY_EXISTS
                        .withDescription("invalid indexName: " + req.getIndexName())
                        .augmentDescription("IllegalArgumentException()")
                        .withCause(e)
                        .asRuntimeException());
            } catch (Exception e) {
                logger.log(Level.WARNING, "error while trying to read index state dir for indexName: " + req.getIndexName(), e);
                responseObserver.onError(Status
                        .INTERNAL
                        .withDescription("error while trying to read index state dir for indexName: " + req.getIndexName() + "at rootDir: ")
                        .augmentDescription("IOException()")
                        .withCause(e)
                        .asRuntimeException());
            }
        }

        @Override
        public void registerFields(FieldDefRequest fieldDefRequest, StreamObserver<FieldDefResponse> responseObserver) {
            try {
                IndexState indexState = globalState.getIndex(fieldDefRequest.getIndexName());
                FieldDefResponse reply = new RegisterFieldsHandler().handle(indexState, fieldDefRequest);
                logger.info("RegisterFieldsHandler registered fields " + reply.toString());
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            } catch (IOException e) {
                logger.log(Level.WARNING, "error while trying to read index state dir for indexName: " + fieldDefRequest.getIndexName(), e);
                responseObserver.onError(Status
                        .INTERNAL
                        .withDescription("error while trying to read index state dir for indexName: " + fieldDefRequest.getIndexName())
                        .augmentDescription("IOException()")
                        .withCause(e)
                        .asRuntimeException());
            } catch (Exception e) {
                logger.log(Level.WARNING, "error while trying to RegisterFields for index " + fieldDefRequest.getIndexName(), e);
                responseObserver.onError(Status
                        .INVALID_ARGUMENT
                        .withDescription("error while trying to RegisterFields for index: " + fieldDefRequest.getIndexName())
                        .augmentDescription(e.getMessage())
                        .asRuntimeException());
            }
        }

        @Override
        public void settings(SettingsRequest settingsRequest, StreamObserver<SettingsResponse> responseObserver) {
            try {
                IndexState indexState = globalState.getIndex(settingsRequest.getIndexName());
                SettingsResponse reply = new SettingsHandler().handle(indexState, settingsRequest);
                logger.info("SettingsHandler returned " + reply.toString());
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            } catch (IOException e) {
                logger.log(Level.WARNING, "error while trying to read index state dir for indexName: " + settingsRequest.getIndexName(), e);
                responseObserver.onError(Status
                        .INTERNAL
                        .withDescription("error while trying to read index state dir for indexName: " + settingsRequest.getIndexName())
                        .augmentDescription("IOException()")
                        .withCause(e)
                        .asRuntimeException());
            } catch (Exception e) {
                logger.log(Level.WARNING, "error while trying to update/get settings for index " + settingsRequest.getIndexName(), e);
                responseObserver.onError(Status
                        .INVALID_ARGUMENT
                        .withDescription("error while trying to update/get settings for index: " + settingsRequest.getIndexName())
                        .augmentDescription(e.getMessage())
                        .asRuntimeException());
            }
        }

        @Override
        public void startIndex(StartIndexRequest startIndexRequest, StreamObserver<StartIndexResponse> responseObserver) {
            IndexState indexState = null;
            try {
                indexState = globalState.getIndex(startIndexRequest.getIndexName());
                StartIndexResponse reply = new StartIndexHandler().handle(indexState, startIndexRequest);
                logger.info("StartIndexHandler returned " + reply.toString());
//                SettingsResponse reply = StartIndexResponse.newBuilder().setResponse(response).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();

            } catch (IOException e) {
                logger.log(Level.WARNING, "error while trying to read index state dir for indexName: " + startIndexRequest.getIndexName(), e);
                responseObserver.onError(Status
                        .INTERNAL
                        .withDescription("error while trying to read index state dir for indexName: " + startIndexRequest.getIndexName())
                        .augmentDescription("IOException()")
                        .withCause(e)
                        .asRuntimeException());
            } catch (Exception e) {
                logger.log(Level.WARNING, "error while trying to start index " + startIndexRequest.getIndexName(), e);
                responseObserver.onError(Status
                        .INVALID_ARGUMENT
                        .withDescription("error while trying to start index: " + startIndexRequest.getIndexName())
                        .augmentDescription(e.getMessage())
                        .asRuntimeException());
            }
        }

        @Override
        public StreamObserver<AddDocumentRequest> addDocuments(StreamObserver<AddDocumentResponse> responseObserver) {
            logger.info("LuceneServerUmesh....addDocuments called");
            return new StreamObserver<AddDocumentRequest>() {
                //TODO make this a config
                private static final int MAX_BUFFER_LEN = 4;
                private long count = 0;
                Queue<AddDocumentRequest> addDocumentRequestQueue = new ArrayBlockingQueue(MAX_BUFFER_LEN);

                @Override
                public void onNext(AddDocumentRequest addDocumentRequest) {
                    logger.info(String.format("onNext, addDocumentRequestQueue size: %s", addDocumentRequestQueue.size()));
                    count++;
                    addDocumentRequestQueue.add(addDocumentRequest);
                    if (addDocumentRequestQueue.size() == MAX_BUFFER_LEN) {
                        logger.info(String.format("indexing addDocumentRequestQueue of size: %s", addDocumentRequestQueue.size()));
                        try {
                            //TODO: non blocking fire and forget via a callable
                            new AddDocumentHandler.DocumentIndexer().runIndexingJob(globalState, addDocumentRequestQueue.stream().collect(Collectors.toList()));
                        } catch (Exception e) {
                            responseObserver.onError(e);
                        } finally {
                            addDocumentRequestQueue.clear();
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.log(Level.WARNING, "addDocuments Cancelled");
                    responseObserver.onError(t);
                }

                @Override
                public void onCompleted() {
                    logger.info(String.format("onCompleted, addDocumentRequestQueue: %s", addDocumentRequestQueue.size()));
                    if (!addDocumentRequestQueue.isEmpty()) {
                        logger.info(String.format("indexing left over addDocumentRequestQueue of size: %s", addDocumentRequestQueue.size()));
                        try {
                            //TODO: make this blocking to ensure all indexing callables (future.get) are done
                            new AddDocumentHandler.DocumentIndexer().runIndexingJob(globalState, addDocumentRequestQueue.stream().collect(Collectors.toList()));
                            responseObserver.onNext(AddDocumentResponse.newBuilder().setGenId(String.valueOf(count)).build());
                            responseObserver.onCompleted();
                        } catch (Exception e) {
                            responseObserver.onError(Status
                                    .PERMISSION_DENIED
                                    .withDescription("error while trying to addDocuments ")
                                    .augmentDescription(e.getMessage())
                                    .withCause(e)
                                    .asRuntimeException());

                        } finally {
                            addDocumentRequestQueue.clear();
                            count = 0;
                        }
                    }
                    //TODO: refresh searcher so freshly indexed data is available.
                }
            };
        }

        @Override
        public void refresh(RefreshRequest refreshRequest, StreamObserver<RefreshResponse> refreshResponseStreamObserver) {
            try {
                IndexState indexState = globalState.getIndex(refreshRequest.getIndexName());
                final ShardState shardState = indexState.getShard(0);
                long t0 = System.nanoTime();
                shardState.maybeRefreshBlocking();
                long t1 = System.nanoTime();
                double refreshTimeMs = (t1 - t0) / 1000000.0;
                RefreshResponse reply = RefreshResponse.newBuilder().setRefreshTimeMS(refreshTimeMs).build();
                logger.info(String.format("RefreshHandler refreshed index: %s in %f", refreshRequest.getIndexName(), refreshTimeMs));
                refreshResponseStreamObserver.onNext(reply);
                refreshResponseStreamObserver.onCompleted();
            } catch (IOException e) {
                logger.log(Level.WARNING, "error while trying to read index state dir for indexName: " + refreshRequest.getIndexName(), e);
                refreshResponseStreamObserver.onError(Status
                        .INTERNAL
                        .withDescription("error while trying to read index state dir for indexName: " + refreshRequest.getIndexName())
                        .augmentDescription(e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
            } catch (Exception e) {
                logger.log(Level.WARNING, "error while trying to refresh index " + refreshRequest.getIndexName(), e);
                refreshResponseStreamObserver.onError(Status
                        .UNKNOWN
                        .withDescription("error while trying to refresh index: " + refreshRequest.getIndexName())
                        .augmentDescription(e.getMessage())
                        .asRuntimeException());
            }
        }

        @Override
        public void commit(CommitRequest commitRequest, StreamObserver<CommitResponse> commitResponseStreamObserver) {
            try {
                IndexState indexState = globalState.getIndex(commitRequest.getIndexName());
                long gen = indexState.commit();
                CommitResponse reply = CommitResponse.newBuilder().setGen(gen).build();
                logger.info(String.format("CommitHandler committed to index: %s for sequenceId: %s", commitRequest.getIndexName(), gen));
                commitResponseStreamObserver.onNext(reply);
                commitResponseStreamObserver.onCompleted();
            } catch (IOException e) {
                logger.log(Level.WARNING, "error while trying to read index state dir for indexName: " + commitRequest.getIndexName(), e);
                commitResponseStreamObserver.onError(Status
                        .INTERNAL
                        .withDescription("error while trying to read index state dir for indexName: " + commitRequest.getIndexName())
                        .augmentDescription(e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
            } catch (Exception e) {
                logger.log(Level.WARNING, "error while trying to commit to  index " + commitRequest.getIndexName(), e);
                commitResponseStreamObserver.onError(Status
                        .UNKNOWN
                        .withDescription("error while trying to commit to index: " + commitRequest.getIndexName())
                        .augmentDescription(e.getMessage())
                        .asRuntimeException());
            }
        }

        @Override
        public void stats(StatsRequest statsRequest, StreamObserver<StatsResponse> statsResponseStreamObserver) {
            try {
                IndexState indexState = globalState.getIndex(statsRequest.getIndexName());
                indexState.verifyStarted();
                StatsResponse.Builder statsResponseBuilder = StatsResponse.newBuilder();
                for (Map.Entry<Integer, ShardState> entry : indexState.shards.entrySet()) {
                    ShardState shardState = entry.getValue();
                    statsResponseBuilder.setOrd(entry.getKey());
                    IndexWriter.DocStats docStats = shardState.writer.getDocStats();
                    statsResponseBuilder.setMaxDoc(docStats.maxDoc);
                    statsResponseBuilder.setNumDocs(docStats.numDocs);
                    // TODO: snapshots

                    // TODO: go per segment and print more details, and
                    // only print segment for a given searcher if it's
                    // "new"

                    // Doesn't actually prune; just gathers stats
                    List<Searcher> tmpSearchers = new ArrayList<>();
                    shardState.slm.prune(new SearcherLifetimeManager.Pruner() {
                        @Override
                        public boolean doPrune(double ageSec, IndexSearcher indexSearcher) {
                            Searcher.Builder searcher = Searcher.newBuilder();
                            searcher.setVersion(((DirectoryReader) indexSearcher.getIndexReader()).getVersion());
                            searcher.setStaleAgeSeconds(ageSec);
                            searcher.setSegments(indexSearcher.getIndexReader().toString());
                            searcher.setNumDocs(indexSearcher.getIndexReader().maxDoc());
                            tmpSearchers.add(searcher.build());
                            return false;
                        }
                    });
                    statsResponseBuilder.addAllSearchers(tmpSearchers);
                    statsResponseBuilder.setState(shardState.getState());

                    SearcherTaxonomyManager.SearcherAndTaxonomy s = shardState.acquire();
                    try {
                        Taxonomy.Builder taxonomy = Taxonomy.newBuilder();
                        taxonomy.setNumOrds(s.taxonomyReader.getSize());
                        taxonomy.setSegments(s.taxonomyReader.toString());
                        statsResponseBuilder.setTaxonomy(taxonomy.build());

                        Searcher.Builder searcher = Searcher.newBuilder();
                        searcher.setSegments(s.searcher.toString());
                        statsResponseBuilder.setCurrentSearcher(searcher.build());
                    } finally {
                        shardState.release(s);
                    }
                }
                StatsResponse reply = statsResponseBuilder.build();
                logger.info(String.format("StatsHandler retrieved stats for index: %s ", reply));
                statsResponseStreamObserver.onNext(reply);
                statsResponseStreamObserver.onCompleted();
            } catch (IOException e) {
                logger.log(Level.WARNING, "error while trying to read index state dir for indexName: " + statsRequest.getIndexName(), e);
                statsResponseStreamObserver.onError(Status
                        .INTERNAL
                        .withDescription("error while trying to read index state dir for indexName: " + statsRequest.getIndexName())
                        .augmentDescription(e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
            } catch (Exception e) {
                logger.log(Level.WARNING, "error while trying to retrieve stats for index " + statsRequest.getIndexName(), e);
                statsResponseStreamObserver.onError(Status
                        .UNKNOWN
                        .withDescription("error while trying to retrieve stats for index: " + statsRequest.getIndexName())
                        .augmentDescription(e.getMessage())
                        .asRuntimeException());
            }
        }

        @Override
        public void search(SearchRequest searchRequest, StreamObserver<SearchResponse> searchResponseStreamObserver) {
            try {
                IndexState indexState = globalState.getIndex(searchRequest.getIndexName());
                SearchHandler searchHandler = new SearchHandler();
                SearchResponse reply = searchHandler.handle(indexState, searchRequest);
                logger.info(String.format("SearchHandler returned results %s", reply.toString()));
                searchResponseStreamObserver.onNext(reply);
                searchResponseStreamObserver.onCompleted();
            } catch (IOException e) {
                logger.log(Level.WARNING, "error while trying to read index state dir for indexName: " + searchRequest.getIndexName(), e);
                searchResponseStreamObserver.onError(Status
                        .INTERNAL
                        .withDescription("error while trying to read index state dir for indexName: " + searchRequest.getIndexName())
                        .augmentDescription(e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
            } catch (Exception e) {
                logger.log(Level.WARNING, String.format("error while trying to execute search %s for index %s", searchRequest.getIndexName(), searchRequest.toString()), e);
                searchResponseStreamObserver.onError(Status
                        .UNKNOWN
                        .withDescription(String.format("error while trying to execute search %s for index %s", searchRequest.getIndexName(), searchRequest.toString()))
                        .augmentDescription(e.getMessage())
                        .asRuntimeException());
            }
        }

    }
}
