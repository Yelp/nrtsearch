/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.grpc;

import static com.yelp.nrtsearch.server.grpc.ReplicationServerClient.MAX_MESSAGE_BYTES_SIZE;

import com.google.api.HttpBody;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.yelp.nrtsearch.LuceneServerModule;
import com.yelp.nrtsearch.server.MetricsRequestHandler;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.AddDocumentHandler.DocumentIndexer;
import com.yelp.nrtsearch.server.luceneserver.AddReplicaHandler;
import com.yelp.nrtsearch.server.luceneserver.BackupIndexRequestHandler;
import com.yelp.nrtsearch.server.luceneserver.BuildSuggestHandler;
import com.yelp.nrtsearch.server.luceneserver.CopyFilesHandler;
import com.yelp.nrtsearch.server.luceneserver.CreateSnapshotHandler;
import com.yelp.nrtsearch.server.luceneserver.DeleteAllDocumentsHandler;
import com.yelp.nrtsearch.server.luceneserver.DeleteByQueryHandler;
import com.yelp.nrtsearch.server.luceneserver.DeleteDocumentsHandler;
import com.yelp.nrtsearch.server.luceneserver.DeleteIndexHandler;
import com.yelp.nrtsearch.server.luceneserver.GetNodesInfoHandler;
import com.yelp.nrtsearch.server.luceneserver.GetStateHandler;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.LiveSettingsHandler;
import com.yelp.nrtsearch.server.luceneserver.NewNRTPointHandler;
import com.yelp.nrtsearch.server.luceneserver.RecvCopyStateHandler;
import com.yelp.nrtsearch.server.luceneserver.RegisterFieldsHandler;
import com.yelp.nrtsearch.server.luceneserver.ReleaseSnapshotHandler;
import com.yelp.nrtsearch.server.luceneserver.ReplicaCurrentSearchingVersionHandler;
import com.yelp.nrtsearch.server.luceneserver.RestoreStateHandler;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.SettingsHandler;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.StartIndexHandler;
import com.yelp.nrtsearch.server.luceneserver.StatsRequestHandler;
import com.yelp.nrtsearch.server.luceneserver.StopIndexHandler;
import com.yelp.nrtsearch.server.luceneserver.SuggestLookupHandler;
import com.yelp.nrtsearch.server.luceneserver.UpdateFieldsHandler;
import com.yelp.nrtsearch.server.luceneserver.UpdateSuggestHandler;
import com.yelp.nrtsearch.server.luceneserver.WriteNRTPointHandler;
import com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefCreator;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.monitoring.Configuration;
import com.yelp.nrtsearch.server.monitoring.LuceneServerMonitoringServerInterceptor;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.PluginsService;
import com.yelp.nrtsearch.server.utils.Archiver;
import com.yelp.nrtsearch.server.utils.ThreadPoolExecutorFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.CollectorRegistry;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/** Server that manages startup/shutdown of a {@code LuceneServer} server. */
public class LuceneServer {
  private static final Logger logger = LoggerFactory.getLogger(LuceneServer.class.getName());
  private final Archiver archiver;
  private final CollectorRegistry collectorRegistry;
  private final PluginsService pluginsService;

  private Server server;
  private Server replicationServer;
  private LuceneServerConfiguration luceneServerConfiguration;

  @Inject
  public LuceneServer(
      LuceneServerConfiguration luceneServerConfiguration,
      Archiver archiver,
      CollectorRegistry collectorRegistry) {
    this.luceneServerConfiguration = luceneServerConfiguration;
    this.archiver = archiver;
    this.collectorRegistry = collectorRegistry;
    this.pluginsService = new PluginsService(luceneServerConfiguration);
  }

  private void start() throws IOException {
    GlobalState globalState = new GlobalState(luceneServerConfiguration);

    List<Plugin> plugins = pluginsService.loadPlugins();

    String serviceName = luceneServerConfiguration.getServiceName();
    String nodeName = luceneServerConfiguration.getNodeName();

    LuceneServerMonitoringServerInterceptor monitoringInterceptor =
        LuceneServerMonitoringServerInterceptor.create(
            Configuration.allMetrics()
                .withLatencyBuckets(luceneServerConfiguration.getMetricsBuckets())
                .withCollectorRegistry(collectorRegistry),
            serviceName,
            nodeName);
    /* The port on which the server should run */
    server =
        ServerBuilder.forPort(luceneServerConfiguration.getPort())
            .addService(
                ServerInterceptors.intercept(
                    new LuceneServerImpl(
                        globalState,
                        luceneServerConfiguration,
                        archiver,
                        collectorRegistry,
                        plugins),
                    monitoringInterceptor))
            .executor(
                ThreadPoolExecutorFactory.getThreadPoolExecutor(
                    ThreadPoolExecutorFactory.ExecutorType.LUCENESERVER,
                    luceneServerConfiguration.getThreadPoolConfiguration()))
            .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
            .build()
            .start();
    logger.info(
        "Server started, listening on " + luceneServerConfiguration.getPort() + " for messages");

    /* The port on which the replication server should run */
    replicationServer =
        ServerBuilder.forPort(luceneServerConfiguration.getReplicationPort())
            .addService(new ReplicationServerImpl(globalState))
            .executor(
                ThreadPoolExecutorFactory.getThreadPoolExecutor(
                    ThreadPoolExecutorFactory.ExecutorType.REPLICATIONSERVER,
                    luceneServerConfiguration.getThreadPoolConfiguration()))
            .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
            .build()
            .start();
    logger.info(
        "Server started, listening on "
            + luceneServerConfiguration.getReplicationPort()
            + " for replication messages");

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                logger.error("*** shutting down gRPC server since JVM is shutting down");
                LuceneServer.this.stop();
                logger.error("*** server shut down");
              }
            });

    if (luceneServerConfiguration.getRestoreState()) {
      logger.info("Loading state for any previously backed up indexes");
      List<String> indexes =
          RestoreStateHandler.restore(
              archiver, globalState, luceneServerConfiguration.getServiceName());
      for (String index : indexes) {
        logger.info("Loaded state for index " + index);
      }
    }
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
    if (replicationServer != null) {
      replicationServer.shutdown();
    }
    pluginsService.shutdown();
  }

  /** Await termination on the main thread since the grpc library uses daemon threads. */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
    if (replicationServer != null) {
      replicationServer.awaitTermination();
    }
  }

  /** Main launches the server from the command line. */
  public static void main(String[] args) {
    System.exit(new CommandLine(new LuceneServerCommand()).execute(args));
  }

  @CommandLine.Command(
      name = "lucene-server",
      mixinStandardHelpOptions = true,
      versionProvider = com.yelp.nrtsearch.server.cli.VersionProvider.class,
      description = "Start NRT search server")
  public static class LuceneServerCommand implements Callable<Integer> {
    @CommandLine.Parameters(
        arity = "0..1",
        paramLabel = "server_yaml_config_file",
        description =
            "Optional yaml config file. Defaults to <resources>/lucene_server_default_configuration.yaml")
    private File optionalConfigFile;

    public Optional<File> maybeConfigFile() {
      return Optional.ofNullable(optionalConfigFile);
    }

    @Override
    public Integer call() throws Exception {
      Injector injector = Guice.createInjector(new LuceneServerModule(this));
      LuceneServer luceneServer = injector.getInstance(LuceneServer.class);
      luceneServer.start();
      luceneServer.blockUntilShutdown();
      return 0;
    }
  }

  static class LuceneServerImpl extends LuceneServerGrpc.LuceneServerImplBase {
    private final GlobalState globalState;
    private final Archiver archiver;
    private final CollectorRegistry collectorRegistry;
    private final ThreadPoolExecutor searchThreadPoolExecutor;

    LuceneServerImpl(
        GlobalState globalState,
        LuceneServerConfiguration configuration,
        Archiver archiver,
        CollectorRegistry collectorRegistry,
        List<Plugin> plugins) {
      this.globalState = globalState;
      this.archiver = archiver;
      this.collectorRegistry = collectorRegistry;
      this.searchThreadPoolExecutor =
          ThreadPoolExecutorFactory.getThreadPoolExecutor(
              ThreadPoolExecutorFactory.ExecutorType.SEARCH,
              globalState.getThreadPoolConfiguration());

      initExtendableComponents(configuration, plugins);
    }

    private void initExtendableComponents(
        LuceneServerConfiguration configuration, List<Plugin> plugins) {
      AnalyzerCreator.initialize(configuration, plugins);
      FieldDefCreator.initialize(configuration, plugins);
      ScriptService.initialize(configuration, plugins);
    }

    @Override
    public void createIndex(
        CreateIndexRequest req, StreamObserver<CreateIndexResponse> responseObserver) {
      IndexState indexState = null;
      try {
        // TODO validate indexName e.g only allow a-z, A-Z, 0-9
        indexState = globalState.createIndex(req.getIndexName(), Paths.get(req.getRootDir()));
        // Create the first shard
        logger.info("NOW ADD SHARD 0");
        indexState.addShard(0, true);
        logger.info("DONE ADD SHARD 0");
        String response =
            String.format(
                "Created Index name: %s, at rootDir: %s", req.getIndexName(), req.getRootDir());
        CreateIndexResponse reply = CreateIndexResponse.newBuilder().setResponse(response).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (IllegalArgumentException e) {
        logger.warn("invalid IndexName: " + req.getIndexName(), e);
        responseObserver.onError(
            Status.ALREADY_EXISTS
                .withDescription("invalid indexName: " + req.getIndexName())
                .augmentDescription("IllegalArgumentException()")
                .withCause(e)
                .asRuntimeException());
      } catch (Exception e) {
        logger.warn(
            "error while trying to save index state to disk for indexName: "
                + req.getIndexName()
                + "at rootDir: "
                + req.getRootDir()
                + req.getIndexName(),
            e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to save index state to disk for indexName: "
                        + req.getIndexName()
                        + "at rootDir: "
                        + req.getRootDir())
                .augmentDescription(e.getMessage())
                .withCause(e)
                .asRuntimeException());
      }
    }

    @Override
    public void liveSettings(
        LiveSettingsRequest req, StreamObserver<LiveSettingsResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(req.getIndexName());
        LiveSettingsResponse reply = new LiveSettingsHandler().handle(indexState, req);
        logger.info("LiveSettingsHandler returned " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (IllegalArgumentException e) {
        logger.warn("index: " + req.getIndexName() + " was not yet created", e);
        responseObserver.onError(
            Status.ALREADY_EXISTS
                .withDescription("invalid indexName: " + req.getIndexName())
                .augmentDescription("IllegalArgumentException()")
                .withCause(e)
                .asRuntimeException());
      } catch (Exception e) {
        logger.warn(
            "error while trying to read index state dir for indexName: " + req.getIndexName(), e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to read index state dir for indexName: "
                        + req.getIndexName()
                        + "at rootDir: ")
                .augmentDescription("IOException()")
                .withCause(e)
                .asRuntimeException());
      }
    }

    @Override
    public void registerFields(
        FieldDefRequest fieldDefRequest, StreamObserver<FieldDefResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(fieldDefRequest.getIndexName());
        FieldDefResponse reply = new RegisterFieldsHandler().handle(indexState, fieldDefRequest);
        logger.info("RegisterFieldsHandler registered fields " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (IOException e) {
        logger.warn(
            "error while trying to read index state dir for indexName: "
                + fieldDefRequest.getIndexName(),
            e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to read index state dir for indexName: "
                        + fieldDefRequest.getIndexName())
                .augmentDescription("IOException()")
                .withCause(e)
                .asRuntimeException());
      } catch (Exception e) {
        logger.warn(
            "error while trying to RegisterFields for index " + fieldDefRequest.getIndexName(), e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(
                    "error while trying to RegisterFields for index: "
                        + fieldDefRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void updateFields(
        FieldDefRequest fieldDefRequest, StreamObserver<FieldDefResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(fieldDefRequest.getIndexName());
        FieldDefResponse reply = new UpdateFieldsHandler().handle(indexState, fieldDefRequest);
        logger.info("UpdateFieldsHandler registered fields " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (IOException e) {
        logger.warn(
            "error while trying to read index state dir for indexName: "
                + fieldDefRequest.getIndexName(),
            e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to read index state dir for indexName: "
                        + fieldDefRequest.getIndexName())
                .augmentDescription("IOException()")
                .withCause(e)
                .asRuntimeException());
      } catch (Exception e) {
        logger.warn(
            "error while trying to UpdateFieldsHandler for index " + fieldDefRequest.getIndexName(),
            e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(
                    "error while trying to UpdateFieldsHandler for index: "
                        + fieldDefRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void settings(
        SettingsRequest settingsRequest, StreamObserver<SettingsResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(settingsRequest.getIndexName());
        SettingsResponse reply = new SettingsHandler().handle(indexState, settingsRequest);
        logger.info("SettingsHandler returned " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (IOException e) {
        logger.warn(
            "error while trying to read index state dir for indexName: "
                + settingsRequest.getIndexName(),
            e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to read index state dir for indexName: "
                        + settingsRequest.getIndexName())
                .augmentDescription("IOException()")
                .withCause(e)
                .asRuntimeException());
      } catch (Exception e) {
        logger.warn(
            "error while trying to update/get settings for index " + settingsRequest.getIndexName(),
            e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(
                    "error while trying to update/get settings for index: "
                        + settingsRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void startIndex(
        StartIndexRequest startIndexRequest, StreamObserver<StartIndexResponse> responseObserver) {
      try {
        IndexState indexState = null;
        StartIndexHandler startIndexHandler = new StartIndexHandler(archiver);
        indexState =
            globalState.getIndex(startIndexRequest.getIndexName(), startIndexRequest.hasRestore());
        StartIndexResponse reply = startIndexHandler.handle(indexState, startIndexRequest);
        logger.info("StartIndexHandler returned " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();

      } catch (IOException e) {
        logger.warn(
            "error while trying to read index state dir for indexName: "
                + startIndexRequest.getIndexName(),
            e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to read index state dir for indexName: "
                        + startIndexRequest.getIndexName())
                .augmentDescription("IOException()")
                .withCause(e)
                .asRuntimeException());
      } catch (Exception e) {
        logger.warn("error while trying to start index " + startIndexRequest.getIndexName(), e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(
                    "error while trying to start index: " + startIndexRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public StreamObserver<AddDocumentRequest> addDocuments(
        StreamObserver<AddDocumentResponse> responseObserver) {

      return new StreamObserver<AddDocumentRequest>() {
        List<Future<Long>> futures = new ArrayList<>();
        // Map of {indexName: addDocumentRequestQueue}
        Map<String, ArrayBlockingQueue<AddDocumentRequest>> addDocumentRequestQueueMap =
            new ConcurrentHashMap<>();
        // Map of {indexName: count}
        Map<String, Long> countMap = new ConcurrentHashMap<>();
        private static final int DEFAULT_MAX_BUFFER_LEN = 100;

        private int getAddDocumentsMaxBufferLen(String indexName) {
          try {
            return globalState.getIndex(indexName).getAddDocumentsMaxBufferLen();
          } catch (Exception e) {
            logger.warn(
                String.format(
                    "error while trying to get addDocumentsMaxBufferLen from"
                        + "liveSettings of index %s. Using DEFAULT_MAX_BUFFER_LEN %d.",
                    indexName, DEFAULT_MAX_BUFFER_LEN),
                e);
            return DEFAULT_MAX_BUFFER_LEN;
          }
        }

        private ArrayBlockingQueue<AddDocumentRequest> getAddDocumentRequestQueue(
            String indexName) {
          if (addDocumentRequestQueueMap.containsKey(indexName)) {
            return addDocumentRequestQueueMap.get(indexName);
          } else {
            int addDocumentsMaxBufferLen = getAddDocumentsMaxBufferLen(indexName);
            ArrayBlockingQueue<AddDocumentRequest> addDocumentRequestQueue =
                new ArrayBlockingQueue<>(addDocumentsMaxBufferLen);
            addDocumentRequestQueueMap.put(indexName, addDocumentRequestQueue);
            return addDocumentRequestQueue;
          }
        }

        private long getCount(String indexName) {
          return countMap.getOrDefault(indexName, 0L);
        }

        private void incrementCount(String indexName) {
          if (countMap.containsKey(indexName)) {
            countMap.put(indexName, countMap.get(indexName) + 1);
          } else {
            countMap.put(indexName, 1L);
          }
        }

        @Override
        public void onNext(AddDocumentRequest addDocumentRequest) {
          String indexName = addDocumentRequest.getIndexName();
          ArrayBlockingQueue<AddDocumentRequest> addDocumentRequestQueue =
              getAddDocumentRequestQueue(indexName);
          logger.debug(
              String.format(
                  "onNext, index: %s, addDocumentRequestQueue size: %s",
                  indexName, addDocumentRequestQueue.size()));
          incrementCount(indexName);
          addDocumentRequestQueue.add(addDocumentRequest);
          if (addDocumentRequestQueue.remainingCapacity() == 0) {
            logger.debug(
                String.format(
                    "indexing addDocumentRequestQueue size: %s, total: %s",
                    addDocumentRequestQueue.size(), getCount(indexName)));
            try {
              List<AddDocumentRequest> addDocRequestList = new ArrayList<>(addDocumentRequestQueue);
              Future<Long> future =
                  globalState.submitIndexingTask(
                      new DocumentIndexer(globalState, addDocRequestList));
              futures.add(future);
            } catch (Exception e) {
              responseObserver.onError(e);
            } finally {
              addDocumentRequestQueue.clear();
            }
          }
        }

        @Override
        public void onError(Throwable t) {
          logger.warn("addDocuments Cancelled");
          responseObserver.onError(t);
        }

        private void onCompletedForIndex(String indexName) {
          ArrayBlockingQueue<AddDocumentRequest> addDocumentRequestQueue =
              getAddDocumentRequestQueue(indexName);
          logger.debug(
              String.format(
                  "onCompleted, addDocumentRequestQueue: %s", addDocumentRequestQueue.size()));
          try {
            // index the left over docs
            if (!addDocumentRequestQueue.isEmpty()) {
              logger.debug(
                  String.format(
                      "indexing left over addDocumentRequestQueue of size: %s",
                      addDocumentRequestQueue.size()));
              List<AddDocumentRequest> addDocRequestList = new ArrayList<>(addDocumentRequestQueue);
              Future<Long> future =
                  globalState.submitIndexingTask(
                      new DocumentIndexer(globalState, addDocRequestList));
              futures.add(future);
            }
            // collect futures, block if needed
            PriorityQueue<Long> pq = new PriorityQueue<>(Collections.reverseOrder());
            int numIndexingChunks = futures.size();
            long t0 = System.nanoTime();
            for (Future<Long> result : futures) {
              Long gen = result.get();
              logger.debug(String.format("Indexing returned sequence-number %s", gen));
              pq.offer(gen);
            }
            long t1 = System.nanoTime();
            responseObserver.onNext(
                AddDocumentResponse.newBuilder().setGenId(String.valueOf(pq.peek())).build());
            responseObserver.onCompleted();
            logger.debug(
                String.format(
                    "Indexing job completed for %s docs, in %s chunks, with latest sequence number: %s, took: %s micro seconds",
                    getCount(indexName), numIndexingChunks, pq.peek(), ((t1 - t0) / 1000)));
          } catch (Exception e) {
            logger.warn("error while trying to addDocuments", e);
            responseObserver.onError(
                Status.INTERNAL
                    .withDescription("error while trying to addDocuments ")
                    .augmentDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());

          } finally {
            addDocumentRequestQueue.clear();
            countMap.put(indexName, 0L);
          }
        }

        @Override
        public void onCompleted() {
          for (String indexName : addDocumentRequestQueueMap.keySet()) {
            onCompletedForIndex(indexName);
          }
        }
      };
    }

    @Override
    public void refresh(
        RefreshRequest refreshRequest,
        StreamObserver<RefreshResponse> refreshResponseStreamObserver) {
      try {
        IndexState indexState = globalState.getIndex(refreshRequest.getIndexName());
        final ShardState shardState = indexState.getShard(0);
        long t0 = System.nanoTime();
        shardState.maybeRefreshBlocking();
        long t1 = System.nanoTime();
        double refreshTimeMs = (t1 - t0) / 1000000.0;
        RefreshResponse reply =
            RefreshResponse.newBuilder().setRefreshTimeMS(refreshTimeMs).build();
        logger.info(
            String.format(
                "RefreshHandler refreshed index: %s in %f",
                refreshRequest.getIndexName(), refreshTimeMs));
        refreshResponseStreamObserver.onNext(reply);
        refreshResponseStreamObserver.onCompleted();
      } catch (IOException e) {
        logger.warn(
            "error while trying to read index state dir for indexName: "
                + refreshRequest.getIndexName(),
            e);
        refreshResponseStreamObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to read index state dir for indexName: "
                        + refreshRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .withCause(e)
                .asRuntimeException());
      } catch (Exception e) {
        logger.warn("error while trying to refresh index " + refreshRequest.getIndexName(), e);
        refreshResponseStreamObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    "error while trying to refresh index: " + refreshRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void commit(
        CommitRequest commitRequest, StreamObserver<CommitResponse> commitResponseStreamObserver) {
      try {
        IndexState indexState = globalState.getIndex(commitRequest.getIndexName());
        long gen = indexState.commit();
        CommitResponse reply = CommitResponse.newBuilder().setGen(gen).build();
        logger.debug(
            String.format(
                "CommitHandler committed to index: %s for sequenceId: %s",
                commitRequest.getIndexName(), gen));
        commitResponseStreamObserver.onNext(reply);
        commitResponseStreamObserver.onCompleted();
      } catch (IOException e) {
        logger.warn(
            "error while trying to read index state dir for indexName: "
                + commitRequest.getIndexName(),
            e);
        commitResponseStreamObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to read index state dir for indexName: "
                        + commitRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .withCause(e)
                .asRuntimeException());
      } catch (Exception e) {
        logger.warn("error while trying to commit to  index " + commitRequest.getIndexName(), e);
        commitResponseStreamObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    "error while trying to commit to index: " + commitRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void stats(
        StatsRequest statsRequest, StreamObserver<StatsResponse> statsResponseStreamObserver) {
      try {
        IndexState indexState = globalState.getIndex(statsRequest.getIndexName());
        indexState.verifyStarted();
        StatsResponse reply = new StatsRequestHandler().handle(indexState, statsRequest);
        logger.debug(String.format("StatsHandler retrieved stats for index: %s ", reply));
        statsResponseStreamObserver.onNext(reply);
        statsResponseStreamObserver.onCompleted();
      } catch (IOException e) {
        logger.warn(
            "error while trying to read index state dir for indexName: "
                + statsRequest.getIndexName(),
            e);
        statsResponseStreamObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to read index state dir for indexName: "
                        + statsRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .withCause(e)
                .asRuntimeException());
      } catch (Exception e) {
        logger.warn(
            "error while trying to retrieve stats for index " + statsRequest.getIndexName(), e);
        statsResponseStreamObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    "error while trying to retrieve stats for index: "
                        + statsRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void search(
        SearchRequest searchRequest, StreamObserver<SearchResponse> searchResponseStreamObserver) {
      try {
        IndexState indexState = globalState.getIndex(searchRequest.getIndexName());
        SearchHandler searchHandler = new SearchHandler(searchThreadPoolExecutor);
        SearchResponse reply = searchHandler.handle(indexState, searchRequest);
        searchResponseStreamObserver.onNext(reply);
        searchResponseStreamObserver.onCompleted();
      } catch (IOException e) {
        logger.warn(
            "error while trying to read index state dir for indexName: "
                + searchRequest.getIndexName(),
            e);
        searchResponseStreamObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to read index state dir for indexName: "
                        + searchRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .withCause(e)
                .asRuntimeException());
      } catch (Exception e) {
        logger.warn(
            String.format(
                "error while trying to execute search %s for index %s",
                searchRequest.getIndexName(), searchRequest.toString()),
            e);
        searchResponseStreamObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    String.format(
                        "error while trying to execute search %s for index %s",
                        searchRequest.getIndexName(), searchRequest.toString()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void delete(
        AddDocumentRequest addDocumentRequest,
        StreamObserver<AddDocumentResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(addDocumentRequest.getIndexName());
        AddDocumentResponse reply =
            new DeleteDocumentsHandler().handle(indexState, addDocumentRequest);
        logger.debug("DeleteDocumentsHandler returned " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            "error while trying to delete documents for index " + addDocumentRequest.getIndexName(),
            e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(
                    "error while trying to delete documents for index: "
                        + addDocumentRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void deleteByQuery(
        DeleteByQueryRequest deleteByQueryRequest,
        StreamObserver<AddDocumentResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(deleteByQueryRequest.getIndexName());
        AddDocumentResponse reply =
            new DeleteByQueryHandler().handle(indexState, deleteByQueryRequest);
        logger.debug("DeleteDocumentsHandler returned " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            "Error while trying to delete documents from index: {}",
            deleteByQueryRequest.getIndexName(),
            e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(
                    "Error while trying to delete documents from index: "
                        + deleteByQueryRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void deleteAll(
        DeleteAllDocumentsRequest deleteAllDocumentsRequest,
        StreamObserver<DeleteAllDocumentsResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(deleteAllDocumentsRequest.getIndexName());
        DeleteAllDocumentsResponse reply =
            new DeleteAllDocumentsHandler().handle(indexState, deleteAllDocumentsRequest);
        logger.info("DeleteAllDocumentsHandler returned " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            "error while trying to deleteAll for index " + deleteAllDocumentsRequest.getIndexName(),
            e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(
                    "error while trying to deleteAll for index: "
                        + deleteAllDocumentsRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void deleteIndex(
        DeleteIndexRequest deleteIndexRequest,
        StreamObserver<DeleteIndexResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(deleteIndexRequest.getIndexName());
        DeleteIndexResponse reply = new DeleteIndexHandler().handle(indexState, deleteIndexRequest);
        logger.info("DeleteAllDocumentsHandler returned " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn("error while trying to delete index " + deleteIndexRequest.getIndexName(), e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(
                    "error while trying to delete index: " + deleteIndexRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void stopIndex(
        StopIndexRequest stopIndexRequest, StreamObserver<DummyResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(stopIndexRequest.getIndexName());
        DummyResponse reply = new StopIndexHandler().handle(indexState, stopIndexRequest);
        logger.info("StopIndexHandler returned " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn("error while trying to stop index " + stopIndexRequest.getIndexName(), e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(
                    "error while trying to stop index: " + stopIndexRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void status(
        HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
      try {
        HealthCheckResponse reply =
            HealthCheckResponse.newBuilder().setHealth(TransferStatusCode.Done).build();
        logger.debug("HealthCheckResponse returned " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn("error while trying to get status", e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("error while trying to get status")
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void buildSuggest(
        BuildSuggestRequest buildSuggestRequest,
        StreamObserver<BuildSuggestResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(buildSuggestRequest.getIndexName());
        BuildSuggestHandler buildSuggestHandler = new BuildSuggestHandler(searchThreadPoolExecutor);
        BuildSuggestResponse reply = buildSuggestHandler.handle(indexState, buildSuggestRequest);
        logger.info(String.format("BuildSuggestHandler returned results %s", reply.toString()));
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            String.format(
                "error while trying to build suggester %s for index %s",
                buildSuggestRequest.getSuggestName(), buildSuggestRequest.getIndexName()),
            e);
        responseObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    String.format(
                        "error while trying to build suggester %s for index %s",
                        buildSuggestRequest.getSuggestName(), buildSuggestRequest.getIndexName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void suggestLookup(
        SuggestLookupRequest suggestLookupRequest,
        StreamObserver<SuggestLookupResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(suggestLookupRequest.getIndexName());
        SuggestLookupHandler suggestLookupHandler = new SuggestLookupHandler();
        SuggestLookupResponse reply = suggestLookupHandler.handle(indexState, suggestLookupRequest);
        logger.info(String.format("SuggestLookupHandler returned results %s", reply.toString()));
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            String.format(
                "error while trying to lookup suggester %s for index %s",
                suggestLookupRequest.getSuggestName(), suggestLookupRequest.getIndexName()),
            e);
        responseObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    String.format(
                        "error while trying to lookup suggester %s for index %s",
                        suggestLookupRequest.getSuggestName(), suggestLookupRequest.getIndexName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void updateSuggest(
        BuildSuggestRequest buildSuggestRequest,
        StreamObserver<BuildSuggestResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(buildSuggestRequest.getIndexName());
        UpdateSuggestHandler updateSuggestHandler = new UpdateSuggestHandler();
        BuildSuggestResponse reply = updateSuggestHandler.handle(indexState, buildSuggestRequest);
        logger.info(String.format("UpdateSuggestHandler returned results %s", reply.toString()));
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            String.format(
                "error while trying to update suggester %s for index %s",
                buildSuggestRequest.getSuggestName(), buildSuggestRequest.getIndexName()),
            e);
        responseObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    String.format(
                        "error while trying to update suggester %s for index %s",
                        buildSuggestRequest.getSuggestName(), buildSuggestRequest.getIndexName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void createSnapshot(
        CreateSnapshotRequest createSnapshotRequest,
        StreamObserver<CreateSnapshotResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(createSnapshotRequest.getIndexName());
        CreateSnapshotHandler createSnapshotHandler = new CreateSnapshotHandler();
        CreateSnapshotResponse reply =
            createSnapshotHandler.handle(indexState, createSnapshotRequest);
        logger.info(String.format("CreateSnapshotHandler returned results %s", reply.toString()));
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            String.format(
                "error while trying to createSnapshot for index %s",
                createSnapshotRequest.getIndexName()),
            e);
        responseObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    String.format(
                        "error while trying to createSnapshot for index %s",
                        createSnapshotRequest.getIndexName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void releaseSnapshot(
        ReleaseSnapshotRequest releaseSnapshotRequest,
        StreamObserver<ReleaseSnapshotResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(releaseSnapshotRequest.getIndexName());
        ReleaseSnapshotHandler releaseSnapshotHandler = new ReleaseSnapshotHandler();
        ReleaseSnapshotResponse reply =
            releaseSnapshotHandler.handle(indexState, releaseSnapshotRequest);
        logger.info(String.format("CreateSnapshotHandler returned results %s", reply.toString()));
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            String.format(
                "error while trying to releaseSnapshot for index %s",
                releaseSnapshotRequest.getIndexName()),
            e);
        responseObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    String.format(
                        "error while trying to releaseSnapshot for index %s",
                        releaseSnapshotRequest.getIndexName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void getAllSnapshotIndexGen(
        GetAllSnapshotGenRequest request,
        StreamObserver<GetAllSnapshotGenResponse> responseObserver) {
      try {
        Set<Long> snapshotGens =
            globalState.getIndex(request.getIndexName()).getShard(0).snapshotGenToVersion.keySet();
        GetAllSnapshotGenResponse response =
            GetAllSnapshotGenResponse.newBuilder().addAllIndexGens(snapshotGens).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } catch (IOException e) {
        logger.error(
            "Error getting all snapshotted index gens for index: {}", request.getIndexName(), e);
        responseObserver.onError(e);
      }
    }

    @Override
    public void backupIndex(
        BackupIndexRequest backupIndexRequest,
        StreamObserver<BackupIndexResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(backupIndexRequest.getIndexName());
        BackupIndexRequestHandler backupIndexRequestHandler =
            new BackupIndexRequestHandler(archiver);
        BackupIndexResponse reply =
            backupIndexRequestHandler.handle(indexState, backupIndexRequest);
        logger.info(String.format("BackupRequestHandler returned results %s", reply.toString()));
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            String.format(
                "error while trying to backupIndex for index: %s for service: %s, resource: %s",
                backupIndexRequest.getIndexName(),
                backupIndexRequest.getServiceName(),
                backupIndexRequest.getResourceName()),
            e);
        responseObserver.onError(
            Status.UNKNOWN
                .withCause(e)
                .withDescription(
                    String.format(
                        "error while trying to backupIndex for index %s for service: %s, resource: %s",
                        backupIndexRequest.getIndexName(),
                        backupIndexRequest.getServiceName(),
                        backupIndexRequest.getResourceName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void metrics(Empty request, StreamObserver<HttpBody> responseObserver) {
      try {
        HttpBody reply = new MetricsRequestHandler(collectorRegistry).process();
        logger.debug("MetricsRequestHandler returned " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn("error while trying to get metrics", e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("error while trying to get metrics")
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void indices(IndicesRequest request, StreamObserver<IndicesResponse> responseObserver) {
      try {
        IndicesResponse reply = StatsRequestHandler.getIndicesResponse(globalState);
        logger.debug("IndicesRequestHandler returned " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn("error while trying to get indices stats", e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("error while trying to get indices stats")
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void state(StateRequest request, StreamObserver<StateResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(request.getIndexName());
        StateResponse reply = new GetStateHandler().handle(indexState, request);
        logger.debug("GetStateHandler returned " + reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn("error while trying to get state for index " + request.getIndexName(), e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(
                    "error while trying to get state for index " + request.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void forceMerge(
        ForceMergeRequest forceMergeRequest, StreamObserver<ForceMergeResponse> responseObserver) {
      if (forceMergeRequest.getIndexName().isEmpty()) {
        responseObserver.onError(new IllegalArgumentException("Index name in request is empty"));
        return;
      }
      if (forceMergeRequest.getMaxNumSegments() == 0) {
        responseObserver.onError(new IllegalArgumentException("Cannot have 0 max segments"));
        return;
      }

      try {
        IndexState indexState = globalState.getIndex(forceMergeRequest.getIndexName());
        ShardState shardState = indexState.shards.get(0);
        shardState.writer.forceMerge(
            forceMergeRequest.getMaxNumSegments(), forceMergeRequest.getDoWait());
      } catch (IOException e) {
        responseObserver.onError(e);
        return;
      }

      ForceMergeResponse.Status status =
          forceMergeRequest.getDoWait()
              ? ForceMergeResponse.Status.FORCE_MERGE_COMPLETED
              : ForceMergeResponse.Status.FORCE_MERGE_SUBMITTED;
      ForceMergeResponse response = ForceMergeResponse.newBuilder().setStatus(status).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  static class ReplicationServerImpl extends ReplicationServerGrpc.ReplicationServerImplBase {
    private final GlobalState globalState;

    public ReplicationServerImpl(GlobalState globalState) {
      this.globalState = globalState;
    }

    @Override
    public void addReplicas(
        AddReplicaRequest addReplicaRequest,
        StreamObserver<AddReplicaResponse> responseStreamObserver) {
      try {
        IndexState indexState = globalState.getIndex(addReplicaRequest.getIndexName());
        AddReplicaResponse reply = new AddReplicaHandler().handle(indexState, addReplicaRequest);
        logger.info("AddReplicaHandler returned " + reply.toString());
        responseStreamObserver.onNext(reply);
        responseStreamObserver.onCompleted();
      } catch (Exception e) {
        logger.warn("error while trying addReplicas " + addReplicaRequest.getIndexName(), e);
        responseStreamObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to addReplicas for index: "
                        + addReplicaRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public StreamObserver<RawFileChunk> sendRawFile(
        StreamObserver<TransferStatus> responseObserver) {
      OutputStream outputStream = null;
      try {
        // TODO: where do we write these files to?
        outputStream = new FileOutputStream(File.createTempFile("tempfile", ".tmp"));
      } catch (IOException e) {
        new RuntimeException(e);
      }
      return new SendRawFileStreamObserver(outputStream, responseObserver);
    }

    static class SendRawFileStreamObserver implements StreamObserver<RawFileChunk> {
      private static final Logger logger =
          LoggerFactory.getLogger(SendRawFileStreamObserver.class.getName());
      private final OutputStream outputStream;
      private final StreamObserver<TransferStatus> responseObserver;
      private final long startTime;

      SendRawFileStreamObserver(
          OutputStream outputStream, StreamObserver<TransferStatus> responseObserver) {
        this.outputStream = outputStream;
        this.responseObserver = responseObserver;
        startTime = System.nanoTime();
      }

      @Override
      public void onNext(RawFileChunk value) {
        // called by client once per chunk of data
        try {
          logger.trace("sendRawFile onNext");
          value.getContent().writeTo(outputStream);
        } catch (IOException e) {
          try {
            outputStream.close();
          } catch (IOException ex) {
            logger.warn("error trying to close outputStream", ex);
          } finally {
            // we either had error in writing to outputStream or cant close it,
            // either case we need to raise it back to client
            responseObserver.onError(e);
          }
        }
      }

      @Override
      public void onError(Throwable t) {
        logger.warn("sendRawFile cancelled", t);
        try {
          outputStream.close();
        } catch (IOException e) {
          logger.warn("error while trying to close outputStream", e);
        } finally {
          // we want to raise error always here
          responseObserver.onError(t);
        }
      }

      @Override
      public void onCompleted() {
        logger.info("sendRawFile completed");
        // called by client after the entire file is sent
        try {
          outputStream.close();
          // TOOD: should we send fileSize copied?
          long endTime = System.nanoTime();
          long totalTimeInMilliSeoncds = (endTime - startTime) / (1000 * 1000);
          responseObserver.onNext(
              TransferStatus.newBuilder()
                  .setCode(TransferStatusCode.Done)
                  .setMessage(String.valueOf(totalTimeInMilliSeoncds))
                  .build());
          responseObserver.onCompleted();
        } catch (IOException e) {
          logger.warn("error while trying to close outputStream", e);
          responseObserver.onError(e);
        }
      }
    }

    @Override
    public void recvRawFile(
        FileInfo fileInfoRequest, StreamObserver<RawFileChunk> rawFileChunkStreamObserver) {
      try {
        IndexState indexState = globalState.getIndex(fileInfoRequest.getIndexName());
        ShardState shardState = indexState.getShard(0);
        IndexInput luceneFile =
            shardState.indexDir.openInput(fileInfoRequest.getFileName(), IOContext.DEFAULT);
        long len = luceneFile.length();
        long pos = fileInfoRequest.getFpStart();
        luceneFile.seek(pos);
        byte[] buffer = new byte[1024 * 64];
        long totalRead;
        totalRead = pos;
        Random random = new Random();
        while (totalRead < len) {
          int chunkSize = (int) Math.min(buffer.length, (len - totalRead));
          luceneFile.readBytes(buffer, 0, chunkSize);
          RawFileChunk rawFileChunk =
              RawFileChunk.newBuilder()
                  .setContent(ByteString.copyFrom(buffer, 0, chunkSize))
                  .build();
          rawFileChunkStreamObserver.onNext(rawFileChunk);
          totalRead += chunkSize;
          randomDelay(random);
        }
        // EOF
        rawFileChunkStreamObserver.onCompleted();
      } catch (Exception e) {
        logger.warn("error on recvRawFile " + fileInfoRequest.getFileName(), e);
        rawFileChunkStreamObserver.onError(
            Status.INTERNAL
                .withDescription("error on recvRawFile: " + fileInfoRequest.getFileName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    /**
     * induces random delay between 1ms to 10ms (both inclusive). Without this excessive buffering
     * happens in server/primary if its to fast compared to receiver/replica. This only happens when
     * we backfill an entire index i.e. very high indexing throughput.
     * https://github.com/grpc/grpc-java/issues/6426. Note that flow control only works with client
     * streaming, whereas we are using unary calls. For unary calls, you can
     *
     * <p>use NettyServerBuilder.maxConcurrentCallsPerConnection to limit concurrent calls
     *
     * <p>slow down to respond so that each request takes a little longer to get response.
     *
     * <p>For client streaming, you can in addition do manual flow control.
     *
     * @param random
     * @throws InterruptedException
     */
    private void randomDelay(Random random) throws InterruptedException {
      int val = random.nextInt(10);
      Thread.sleep(val + 1);
    }

    @Override
    public void recvCopyState(
        CopyStateRequest request, StreamObserver<CopyState> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(request.getIndexName());
        CopyState reply = new RecvCopyStateHandler().handle(indexState, request);
        logger.debug(
            "RecvCopyStateHandler returned, completedMergeFiles count: "
                + reply.getCompletedMergeFilesCount());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            String.format(
                "error on recvCopyState for replicaId: %s, for index: %s",
                request.getReplicaId(), request.getIndexName()),
            e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    String.format(
                        "error on recvCopyState for replicaId: %s, for index: %s",
                        request.getReplicaId(), request.getIndexName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void copyFiles(CopyFiles request, StreamObserver<TransferStatus> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(request.getIndexName());
        CopyFilesHandler copyFilesHandler = new CopyFilesHandler();
        // we need to send multiple responses to client from this method
        copyFilesHandler.handle(indexState, request, responseObserver);
        logger.info("CopyFilesHandler returned successfully");
      } catch (Exception e) {
        logger.warn(
            String.format(
                "error on copyFiles for primaryGen: %s, for index: %s",
                request.getPrimaryGen(), request.getIndexName()),
            e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    String.format(
                        "error on copyFiles for primaryGen: %s, for index: %s",
                        request.getPrimaryGen(), request.getIndexName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void newNRTPoint(NewNRTPoint request, StreamObserver<TransferStatus> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(request.getIndexName());
        NewNRTPointHandler newNRTPointHander = new NewNRTPointHandler();
        TransferStatus reply = newNRTPointHander.handle(indexState, request);
        logger.debug(
            "NewNRTPointHandler returned status "
                + reply.getCode()
                + " message: "
                + reply.getMessage());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            String.format(
                "error on newNRTPoint for indexName: %s, for version: %s, primaryGen: %s",
                request.getIndexName(), request.getVersion(), request.getPrimaryGen()),
            e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    String.format(
                        "error on newNRTPoint for indexName: %s, for version: %s, primaryGen: %s",
                        request.getIndexName(), request.getVersion(), request.getPrimaryGen()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void writeNRTPoint(
        IndexName indexNameRequest, StreamObserver<SearcherVersion> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(indexNameRequest.getIndexName());
        WriteNRTPointHandler writeNRTPointHander = new WriteNRTPointHandler();
        SearcherVersion reply = writeNRTPointHander.handle(indexState, indexNameRequest);
        logger.debug("WriteNRTPointHandler returned version " + reply.getVersion());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            String.format(
                "error on writeNRTPoint for indexName: %s", indexNameRequest.getIndexName()),
            e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    String.format(
                        "error on writeNRTPoint for indexName: %s",
                        indexNameRequest.getIndexName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void getCurrentSearcherVersion(
        IndexName indexNameRequest, StreamObserver<SearcherVersion> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(indexNameRequest.getIndexName());
        ReplicaCurrentSearchingVersionHandler replicaCurrentSearchingVersionHandler =
            new ReplicaCurrentSearchingVersionHandler();
        SearcherVersion reply =
            replicaCurrentSearchingVersionHandler.handle(indexState, indexNameRequest);
        logger.info("ReplicaCurrentSearchingVersionHandler returned version " + reply.getVersion());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            String.format(
                "error on getCurrentSearcherVersion for indexName: %s",
                indexNameRequest.getIndexName()),
            e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    String.format(
                        "error on getCurrentSearcherVersion for indexName: %s",
                        indexNameRequest.getIndexName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void getConnectedNodes(
        GetNodesRequest getNodesRequest, StreamObserver<GetNodesResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(getNodesRequest.getIndexName());
        GetNodesResponse reply = new GetNodesInfoHandler().handle(indexState, getNodesRequest);
        logger.debug(
            "GetNodesInfoHandler returned GetNodeResponse of size " + reply.getNodesCount());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn("error on GetNodesInfoHandler", e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(String.format("error on GetNodesInfoHandler"))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }
  }
}
