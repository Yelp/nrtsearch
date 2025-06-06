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
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.concurrent.ExecutorFactory;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.config.QueryCacheConfig;
import com.yelp.nrtsearch.server.custom.request.CustomRequestProcessor;
import com.yelp.nrtsearch.server.field.FieldDefCreator;
import com.yelp.nrtsearch.server.handler.AddDocumentHandler;
import com.yelp.nrtsearch.server.handler.AddReplicaHandler;
import com.yelp.nrtsearch.server.handler.BackupWarmingQueriesHandler;
import com.yelp.nrtsearch.server.handler.CommitHandler;
import com.yelp.nrtsearch.server.handler.CopyFilesHandler;
import com.yelp.nrtsearch.server.handler.CreateIndexHandler;
import com.yelp.nrtsearch.server.handler.CreateSnapshotHandler;
import com.yelp.nrtsearch.server.handler.CustomHandler;
import com.yelp.nrtsearch.server.handler.DeleteAllDocumentsHandler;
import com.yelp.nrtsearch.server.handler.DeleteByQueryHandler;
import com.yelp.nrtsearch.server.handler.DeleteDocumentsHandler;
import com.yelp.nrtsearch.server.handler.DeleteIndexHandler;
import com.yelp.nrtsearch.server.handler.ForceMergeDeletesHandler;
import com.yelp.nrtsearch.server.handler.ForceMergeHandler;
import com.yelp.nrtsearch.server.handler.GetAllSnapshotIndexGenHandler;
import com.yelp.nrtsearch.server.handler.GetNodesInfoHandler;
import com.yelp.nrtsearch.server.handler.GetStateHandler;
import com.yelp.nrtsearch.server.handler.GlobalStateHandler;
import com.yelp.nrtsearch.server.handler.Handler;
import com.yelp.nrtsearch.server.handler.IndexStateHandler;
import com.yelp.nrtsearch.server.handler.IndicesHandler;
import com.yelp.nrtsearch.server.handler.LiveSettingsHandler;
import com.yelp.nrtsearch.server.handler.LiveSettingsV2Handler;
import com.yelp.nrtsearch.server.handler.MetricsHandler;
import com.yelp.nrtsearch.server.handler.NewNRTPointHandler;
import com.yelp.nrtsearch.server.handler.NodeInfoHandler;
import com.yelp.nrtsearch.server.handler.ReadyHandler;
import com.yelp.nrtsearch.server.handler.RecvCopyStateHandler;
import com.yelp.nrtsearch.server.handler.RecvRawFileHandler;
import com.yelp.nrtsearch.server.handler.RecvRawFileV2Handler;
import com.yelp.nrtsearch.server.handler.RefreshHandler;
import com.yelp.nrtsearch.server.handler.RegisterFieldsHandler;
import com.yelp.nrtsearch.server.handler.ReleaseSnapshotHandler;
import com.yelp.nrtsearch.server.handler.ReloadStateHandler;
import com.yelp.nrtsearch.server.handler.ReplicaCurrentSearchingVersionHandler;
import com.yelp.nrtsearch.server.handler.SearchHandler;
import com.yelp.nrtsearch.server.handler.SearchV2Handler;
import com.yelp.nrtsearch.server.handler.SendRawFileHandler;
import com.yelp.nrtsearch.server.handler.SettingsHandler;
import com.yelp.nrtsearch.server.handler.SettingsV2Handler;
import com.yelp.nrtsearch.server.handler.StartIndexHandler;
import com.yelp.nrtsearch.server.handler.StartIndexV2Handler;
import com.yelp.nrtsearch.server.handler.StatsHandler;
import com.yelp.nrtsearch.server.handler.StatusHandler;
import com.yelp.nrtsearch.server.handler.StopIndexHandler;
import com.yelp.nrtsearch.server.handler.UpdateFieldsHandler;
import com.yelp.nrtsearch.server.handler.WriteNRTPointHandler;
import com.yelp.nrtsearch.server.highlights.HighlighterService;
import com.yelp.nrtsearch.server.logging.HitsLoggerCreator;
import com.yelp.nrtsearch.server.modules.NrtsearchModule;
import com.yelp.nrtsearch.server.monitoring.BootstrapMetrics;
import com.yelp.nrtsearch.server.monitoring.Configuration;
import com.yelp.nrtsearch.server.monitoring.DeadlineMetrics;
import com.yelp.nrtsearch.server.monitoring.DirSizeCollector;
import com.yelp.nrtsearch.server.monitoring.IndexMetrics;
import com.yelp.nrtsearch.server.monitoring.IndexingMetrics;
import com.yelp.nrtsearch.server.monitoring.MergeSchedulerCollector;
import com.yelp.nrtsearch.server.monitoring.NrtMetrics;
import com.yelp.nrtsearch.server.monitoring.NrtsearchMonitoringServerInterceptor;
import com.yelp.nrtsearch.server.monitoring.ProcStatCollector;
import com.yelp.nrtsearch.server.monitoring.QueryCacheCollector;
import com.yelp.nrtsearch.server.monitoring.SearchResponseCollector;
import com.yelp.nrtsearch.server.monitoring.ThreadPoolCollector;
import com.yelp.nrtsearch.server.monitoring.ThreadPoolCollector.RejectionCounterWrapper;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.PluginsService;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.rescore.RescorerCreator;
import com.yelp.nrtsearch.server.script.ScriptService;
import com.yelp.nrtsearch.server.search.FetchTaskCreator;
import com.yelp.nrtsearch.server.search.cache.NrtQueryCache;
import com.yelp.nrtsearch.server.search.collectors.CollectorCreator;
import com.yelp.nrtsearch.server.similarity.SimilarityCreator;
import com.yelp.nrtsearch.server.state.GlobalState;
import com.yelp.nrtsearch.tools.cli.VersionProvider;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/** Server that manages startup/shutdown of a {@code NrtsearchServer} server. */
public class NrtsearchServer {
  private static final Logger logger = LoggerFactory.getLogger(NrtsearchServer.class.getName());
  private final RemoteBackend remoteBackend;
  private final PrometheusRegistry prometheusRegistry;
  private final PluginsService pluginsService;

  private Server server;
  private Server replicationServer;
  private final NrtsearchConfig configuration;

  @Inject
  public NrtsearchServer(
      NrtsearchConfig configuration,
      RemoteBackend remoteBackend,
      PrometheusRegistry prometheusRegistry) {
    this.configuration = configuration;
    this.remoteBackend = remoteBackend;
    this.prometheusRegistry = prometheusRegistry;
    this.pluginsService = new PluginsService(configuration, remoteBackend, prometheusRegistry);
  }

  @VisibleForTesting
  public void start() throws IOException {
    long startNs = System.nanoTime();
    List<Plugin> plugins = pluginsService.loadPlugins();

    LuceneServerImpl serverImpl =
        new LuceneServerImpl(configuration, remoteBackend, prometheusRegistry, plugins);
    GlobalState globalState = serverImpl.getGlobalState();

    registerMetrics(globalState);

    if (configuration.getMaxConcurrentCallsPerConnectionForReplication() != -1) {
      replicationServer =
          NettyServerBuilder.forPort(configuration.getReplicationPort())
              .addService(
                  new ReplicationServerImpl(
                      globalState, configuration.getVerifyReplicationIndexId()))
              .executor(
                  ExecutorFactory.getInstance()
                      .getExecutor(ExecutorFactory.ExecutorType.REPLICATIONSERVER))
              .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
              .maxConcurrentCallsPerConnection(
                  configuration.getMaxConcurrentCallsPerConnectionForReplication())
              .maxConnectionAge(configuration.getMaxConnectionAgeForReplication(), TimeUnit.SECONDS)
              .maxConnectionAgeGrace(
                  configuration.getMaxConnectionAgeGraceForReplication(), TimeUnit.SECONDS)
              .build()
              .start();
    } else {
      replicationServer =
          ServerBuilder.forPort(configuration.getReplicationPort())
              .addService(
                  new ReplicationServerImpl(
                      globalState, configuration.getVerifyReplicationIndexId()))
              .executor(
                  ExecutorFactory.getInstance()
                      .getExecutor(ExecutorFactory.ExecutorType.REPLICATIONSERVER))
              .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
              .build()
              .start();
    }
    logger.info(
        "Server started, listening on "
            + configuration.getReplicationPort()
            + " for replication messages");

    // Inform global state that the replication server is started, and it is safe to start indices
    globalState.replicationStarted(replicationServer.getPort());

    NrtsearchMonitoringServerInterceptor monitoringInterceptor =
        NrtsearchMonitoringServerInterceptor.create(
            Configuration.allMetrics()
                .withLatencyBuckets(configuration.getMetricsBuckets())
                .withPrometheusRegistry(prometheusRegistry));
    /* The port on which the server should run */
    GrpcServerExecutorSupplier executorSupplier = new GrpcServerExecutorSupplier();
    server =
        ServerBuilder.forPort(configuration.getPort())
            // The last interceptor is invoked first
            .addService(
                ServerInterceptors.intercept(
                    serverImpl, new NrtsearchHeaderInterceptor(), monitoringInterceptor))
            .addService(ProtoReflectionService.newInstance())
            // Set executor supplier to use different thread pool for metrics method
            .callExecutor(executorSupplier)
            // We still need this executor to run tasks before the point when executorSupplier can
            // be called (https://github.com/grpc/grpc-java/issues/8274)
            .executor(executorSupplier.getGrpcExecutor())
            .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
            .compressorRegistry(LuceneServerStubBuilder.COMPRESSOR_REGISTRY)
            .decompressorRegistry(LuceneServerStubBuilder.DECOMPRESSOR_REGISTRY)
            .build()
            .start();
    logger.info("Server started, listening on " + configuration.getPort() + " for messages");
    BootstrapMetrics.nrtsearchBootstrapTimer.set((System.nanoTime() - startNs) / 1_000_000_000.0);
  }

  @VisibleForTesting
  public void stop() {
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

  /** Register prometheus metrics exposed by /status/metrics */
  private void registerMetrics(GlobalState globalState) {
    // register jvm metrics
    JvmMetrics.builder().register(prometheusRegistry);
    // register thread pool metrics
    prometheusRegistry.register(new ThreadPoolCollector());
    prometheusRegistry.register(RejectionCounterWrapper.rejectionCounter);
    // register bootstrap metrics
    BootstrapMetrics.register(prometheusRegistry);
    // register nrt metrics
    NrtMetrics.register(prometheusRegistry);
    // register index metrics
    IndexMetrics.register(prometheusRegistry);
    // register query cache metrics
    prometheusRegistry.register(new QueryCacheCollector());
    // register deadline cancellation metrics
    DeadlineMetrics.register(prometheusRegistry);
    // register directory size metrics
    prometheusRegistry.register(new DirSizeCollector(globalState));
    prometheusRegistry.register(new ProcStatCollector());
    prometheusRegistry.register(new MergeSchedulerCollector(globalState));
    prometheusRegistry.register(new SearchResponseCollector(globalState));
    // register Indexing metrics such as individual addDocument, updateDocValue latencies and qps
    IndexingMetrics.register(prometheusRegistry);
  }

  /** Main launches the server from the command line. */
  public static void main(String[] args) {
    System.exit(new CommandLine(new NrtsearchServerCommand()).execute(args));
  }

  @CommandLine.Command(
      name = "nrtsearch_server",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class,
      description = "Start NRT search server")
  public static class NrtsearchServerCommand implements Callable<Integer> {
    @CommandLine.Parameters(
        arity = "0..1",
        paramLabel = "server_yaml_config_file",
        description =
            "Optional yaml config file. Defaults to <resources>/nrtsearch_default_config.yaml")
    private File optionalConfigFile;

    public Optional<File> maybeConfigFile() {
      return Optional.ofNullable(optionalConfigFile);
    }

    @Override
    public Integer call() throws Exception {
      NrtsearchServer server;
      try {
        Injector injector = Guice.createInjector(new NrtsearchModule(this));
        server = injector.getInstance(NrtsearchServer.class);
        server.start();

        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(
                    () -> {
                      // Use stderr here since the logger may have been reset by its JVM shutdown
                      // hook.
                      logger.error("*** shutting down gRPC server since JVM is shutting down");
                      server.stop();
                      logger.error("*** server shut down");
                    }));
      } catch (Throwable t) {
        logger.error("Uncaught exception", t);
        throw t;
      }
      server.blockUntilShutdown();
      return 0;
    }
  }

  static class LuceneServerImpl extends LuceneServerGrpc.LuceneServerImplBase {
    private final JsonFormat.Printer protoMessagePrinter =
        JsonFormat.printer().omittingInsignificantWhitespace();
    private final GlobalState globalState;

    private final AddDocumentHandler addDocumentHandler;
    private final BackupWarmingQueriesHandler backupWarmingQueriesHandler;
    private final CommitHandler commitHandler;
    private final CreateIndexHandler createIndexHandler;
    private final CreateSnapshotHandler createSnapshotHandler;
    private final CustomHandler customHandler;
    private final DeleteAllDocumentsHandler deleteAllDocumentsHandler;
    private final DeleteByQueryHandler deleteByQueryHandler;
    private final DeleteDocumentsHandler deleteDocumentsHandler;
    private final DeleteIndexHandler deleteIndexHandler;
    private final ForceMergeDeletesHandler forceMergeDeletesHandler;
    private final ForceMergeHandler forceMergeHandler;
    private final GetAllSnapshotIndexGenHandler getAllSnapshotIndexGenHandler;
    private final GetStateHandler getStateHandler;
    private final GlobalStateHandler globalStateHandler;
    private final IndexStateHandler indexStateHandler;
    private final IndicesHandler indicesHandler;
    private final LiveSettingsHandler liveSettingsHandler;
    private final LiveSettingsV2Handler liveSettingsV2Handler;
    private final MetricsHandler metricsHandler;
    private final NodeInfoHandler nodeInfoHandler;
    private final ReadyHandler readyHandler;
    private final RefreshHandler refreshHandler;
    private final RegisterFieldsHandler registerFieldsHandler;
    private final ReleaseSnapshotHandler releaseSnapshotHandler;
    private final ReloadStateHandler reloadStateHandler;
    private final SearchHandler searchHandler;
    private final SearchV2Handler searchV2Handler;
    private final SettingsHandler settingsHandler;
    private final SettingsV2Handler settingsV2Handler;
    private final StartIndexHandler startIndexHandler;
    private final StartIndexV2Handler startIndexV2Handler;
    private final StatsHandler statsHandler;
    private final StatusHandler statusHandler;
    private final StopIndexHandler stopIndexHandler;
    private final UpdateFieldsHandler updateFieldsHandler;

    /**
     * Constructor used with newer state handling. Defers initialization of global state until after
     * extendable components.
     *
     * @param configuration server configuration
     * @param remoteBackend backend for persistent remote storage
     * @param prometheusRegistry metrics collector registry
     * @param plugins loaded plugins
     * @throws IOException
     */
    LuceneServerImpl(
        NrtsearchConfig configuration,
        RemoteBackend remoteBackend,
        PrometheusRegistry prometheusRegistry,
        List<Plugin> plugins)
        throws IOException {

      DeadlineUtils.setCancellationEnabled(configuration.getDeadlineCancellation());
      ExecutorFactory.init(configuration.getThreadPoolConfiguration());

      initQueryCache(configuration);
      initMaxClauseCount(configuration);
      initExtendableComponents(configuration, plugins);

      this.globalState = GlobalState.createState(configuration, remoteBackend);

      // Initialize handlers
      addDocumentHandler = new AddDocumentHandler(globalState);
      backupWarmingQueriesHandler = new BackupWarmingQueriesHandler(globalState);
      commitHandler = new CommitHandler(globalState);
      createIndexHandler = new CreateIndexHandler(globalState);
      createSnapshotHandler = new CreateSnapshotHandler(globalState);
      customHandler = new CustomHandler(globalState);
      deleteAllDocumentsHandler = new DeleteAllDocumentsHandler(globalState);
      deleteByQueryHandler = new DeleteByQueryHandler(globalState);
      deleteDocumentsHandler = new DeleteDocumentsHandler(globalState);
      deleteIndexHandler = new DeleteIndexHandler(globalState);
      forceMergeDeletesHandler = new ForceMergeDeletesHandler(globalState);
      forceMergeHandler = new ForceMergeHandler(globalState);
      getAllSnapshotIndexGenHandler = new GetAllSnapshotIndexGenHandler(globalState);
      getStateHandler = new GetStateHandler(globalState);
      globalStateHandler = new GlobalStateHandler(globalState);
      indexStateHandler = new IndexStateHandler(globalState);
      indicesHandler = new IndicesHandler(globalState);
      liveSettingsHandler = new LiveSettingsHandler(globalState);
      liveSettingsV2Handler = new LiveSettingsV2Handler(globalState);
      metricsHandler = new MetricsHandler(prometheusRegistry);
      nodeInfoHandler = new NodeInfoHandler(globalState);
      readyHandler = new ReadyHandler(globalState);
      refreshHandler = new RefreshHandler(globalState);
      registerFieldsHandler = new RegisterFieldsHandler(globalState);
      releaseSnapshotHandler = new ReleaseSnapshotHandler(globalState);
      reloadStateHandler = new ReloadStateHandler(globalState);
      searchHandler = new SearchHandler(globalState);
      searchV2Handler = new SearchV2Handler(globalState, searchHandler);
      settingsHandler = new SettingsHandler(globalState);
      settingsV2Handler = new SettingsV2Handler(globalState);
      startIndexHandler = new StartIndexHandler(globalState);
      startIndexV2Handler = new StartIndexV2Handler(globalState);
      statsHandler = new StatsHandler(globalState);
      statusHandler = new StatusHandler();
      stopIndexHandler = new StopIndexHandler(globalState);
      updateFieldsHandler = new UpdateFieldsHandler(globalState);
    }

    @VisibleForTesting
    static void initQueryCache(NrtsearchConfig configuration) {
      QueryCacheConfig cacheConfig = configuration.getQueryCacheConfig();
      QueryCache queryCache = null;
      if (cacheConfig.getEnabled()) {
        queryCache =
            new NrtQueryCache(
                cacheConfig.getMaxQueries(),
                cacheConfig.getMaxMemoryBytes(),
                cacheConfig.getLeafPredicate(),
                cacheConfig.getSkipCacheFactor());
      }
      IndexSearcher.setDefaultQueryCache(queryCache);
    }

    @VisibleForTesting
    static void initMaxClauseCount(NrtsearchConfig configuration) {
      IndexSearcher.setMaxClauseCount(configuration.getMaxClauseCount());
    }

    private void initExtendableComponents(NrtsearchConfig configuration, List<Plugin> plugins) {
      // this block should be in alphabetical order
      AnalyzerCreator.initialize(configuration, plugins);
      CollectorCreator.initialize(configuration, plugins);
      CustomRequestProcessor.initialize(configuration, plugins);
      FetchTaskCreator.initialize(configuration, plugins);
      FieldDefCreator.initialize(configuration, plugins);
      HighlighterService.initialize(configuration, plugins);
      HitsLoggerCreator.initialize(configuration, plugins);
      RescorerCreator.initialize(configuration, plugins);
      ScriptService.initialize(configuration, plugins);
      SimilarityCreator.initialize(configuration, plugins);
    }

    /** Get the global cluster state. */
    public GlobalState getGlobalState() {
      return globalState;
    }

    @Override
    public void createIndex(
        CreateIndexRequest req, StreamObserver<CreateIndexResponse> responseObserver) {
      Handler.handleUnaryRequest("createIndex", req, responseObserver, createIndexHandler);
    }

    @Override
    public void liveSettings(
        LiveSettingsRequest req, StreamObserver<LiveSettingsResponse> responseObserver) {
      Handler.handleUnaryRequest("liveSettings", req, responseObserver, liveSettingsHandler);
    }

    @Override
    public void liveSettingsV2(
        LiveSettingsV2Request req, StreamObserver<LiveSettingsV2Response> responseObserver) {
      Handler.handleUnaryRequest("liveSettingsV2", req, responseObserver, liveSettingsV2Handler);
    }

    @Override
    public void registerFields(
        FieldDefRequest fieldDefRequest, StreamObserver<FieldDefResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "registerFields", fieldDefRequest, responseObserver, registerFieldsHandler);
    }

    @Override
    public void updateFields(
        FieldDefRequest fieldDefRequest, StreamObserver<FieldDefResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "updateFields", fieldDefRequest, responseObserver, updateFieldsHandler);
    }

    @Override
    public void settings(
        SettingsRequest settingsRequest, StreamObserver<SettingsResponse> responseObserver) {
      Handler.handleUnaryRequest("settings", settingsRequest, responseObserver, settingsHandler);
    }

    @Override
    public void settingsV2(
        SettingsV2Request settingsRequest, StreamObserver<SettingsV2Response> responseObserver) {
      Handler.handleUnaryRequest(
          "settingsV2", settingsRequest, responseObserver, settingsV2Handler);
    }

    @Override
    public void startIndex(
        StartIndexRequest startIndexRequest, StreamObserver<StartIndexResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "startIndex", startIndexRequest, responseObserver, startIndexHandler);
    }

    @Override
    public void startIndexV2(
        StartIndexV2Request startIndexRequest,
        StreamObserver<StartIndexResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "startIndexV2", startIndexRequest, responseObserver, startIndexV2Handler);
    }

    @Override
    public StreamObserver<AddDocumentRequest> addDocuments(
        StreamObserver<AddDocumentResponse> responseObserver) {
      return addDocumentHandler.handle(responseObserver);
    }

    @Override
    public void refresh(
        RefreshRequest refreshRequest,
        StreamObserver<RefreshResponse> refreshResponseStreamObserver) {
      Handler.handleUnaryRequest(
          "refresh", refreshRequest, refreshResponseStreamObserver, refreshHandler);
    }

    @Override
    public void commit(
        CommitRequest commitRequest, StreamObserver<CommitResponse> commitResponseStreamObserver) {
      commitHandler.handle(commitRequest, commitResponseStreamObserver);
    }

    @Override
    public void stats(
        StatsRequest statsRequest, StreamObserver<StatsResponse> statsResponseStreamObserver) {
      Handler.handleUnaryRequest("stats", statsRequest, statsResponseStreamObserver, statsHandler);
    }

    @Override
    public void search(
        SearchRequest searchRequest, StreamObserver<SearchResponse> responseObserver) {
      searchHandler.handle(searchRequest, responseObserver);
    }

    @Override
    public void searchV2(
        SearchRequest searchRequest, StreamObserver<Any> searchResponseStreamObserver) {
      searchV2Handler.handle(searchRequest, searchResponseStreamObserver);
    }

    @Override
    public void delete(
        AddDocumentRequest addDocumentRequest,
        StreamObserver<AddDocumentResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "delete", addDocumentRequest, responseObserver, deleteDocumentsHandler);
    }

    @Override
    public void deleteByQuery(
        DeleteByQueryRequest deleteByQueryRequest,
        StreamObserver<AddDocumentResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "deleteByQuery", deleteByQueryRequest, responseObserver, deleteByQueryHandler);
    }

    @Override
    public void deleteAll(
        DeleteAllDocumentsRequest deleteAllDocumentsRequest,
        StreamObserver<DeleteAllDocumentsResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "deleteAll", deleteAllDocumentsRequest, responseObserver, deleteAllDocumentsHandler);
    }

    @Override
    public void deleteIndex(
        DeleteIndexRequest deleteIndexRequest,
        StreamObserver<DeleteIndexResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "deleteIndex", deleteIndexRequest, responseObserver, deleteIndexHandler);
    }

    @Override
    public void stopIndex(
        StopIndexRequest stopIndexRequest, StreamObserver<DummyResponse> responseObserver) {
      Handler.handleUnaryRequest("stopIndex", stopIndexRequest, responseObserver, stopIndexHandler);
    }

    @Override
    public void reloadState(
        ReloadStateRequest request, StreamObserver<ReloadStateResponse> responseObserver) {
      Handler.handleUnaryRequest("reloadState", request, responseObserver, reloadStateHandler);
    }

    @Override
    public void status(
        HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
      Handler.handleUnaryRequest("status", request, responseObserver, statusHandler);
    }

    /**
     * Returns a valid response only if all indices in {@link GlobalState} are started or if any
     * index names are provided in {@link ReadyCheckRequest} returns a valid response if those
     * specific indices are started.
     */
    @Override
    public void ready(
        ReadyCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
      Handler.handleUnaryRequest("ready", request, responseObserver, readyHandler);
    }

    @Override
    public void createSnapshot(
        CreateSnapshotRequest createSnapshotRequest,
        StreamObserver<CreateSnapshotResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "createSnapshot", createSnapshotRequest, responseObserver, createSnapshotHandler);
    }

    @Override
    public void releaseSnapshot(
        ReleaseSnapshotRequest releaseSnapshotRequest,
        StreamObserver<ReleaseSnapshotResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "releaseSnapshot", releaseSnapshotRequest, responseObserver, releaseSnapshotHandler);
    }

    @Override
    public void getAllSnapshotIndexGen(
        GetAllSnapshotGenRequest request,
        StreamObserver<GetAllSnapshotGenResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "getAllSnapshotIndexGen", request, responseObserver, getAllSnapshotIndexGenHandler);
    }

    @Override
    public void backupWarmingQueries(
        BackupWarmingQueriesRequest request,
        StreamObserver<BackupWarmingQueriesResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "backupWarmingQueries", request, responseObserver, backupWarmingQueriesHandler);
    }

    @Override
    public void metrics(Empty request, StreamObserver<HttpBody> responseObserver) {
      Handler.handleUnaryRequest("metrics", request, responseObserver, metricsHandler);
    }

    @Override
    public void indices(IndicesRequest request, StreamObserver<IndicesResponse> responseObserver) {
      Handler.handleUnaryRequest("indices", request, responseObserver, indicesHandler);
    }

    @Override
    public void nodeInfo(
        NodeInfoRequest request, StreamObserver<NodeInfoResponse> responseStreamObserver) {
      Handler.handleUnaryRequest("nodeInfo", request, responseStreamObserver, nodeInfoHandler);
    }

    @Override
    public void globalState(
        GlobalStateRequest request, StreamObserver<GlobalStateResponse> responseObserver) {
      Handler.handleUnaryRequest("globalState", request, responseObserver, globalStateHandler);
    }

    @Override
    public void state(StateRequest request, StreamObserver<StateResponse> responseObserver) {
      Handler.handleUnaryRequest("state", request, responseObserver, getStateHandler);
    }

    @Override
    public void indexState(
        IndexStateRequest request, StreamObserver<IndexStateResponse> responseStreamObserver) {
      Handler.handleUnaryRequest("indexState", request, responseStreamObserver, indexStateHandler);
    }

    @Override
    public void forceMerge(
        ForceMergeRequest forceMergeRequest, StreamObserver<ForceMergeResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "forceMerge", forceMergeRequest, responseObserver, forceMergeHandler);
    }

    @Override
    public void forceMergeDeletes(
        ForceMergeDeletesRequest forceMergeRequest,
        StreamObserver<ForceMergeDeletesResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "forceMergeDeletes", forceMergeRequest, responseObserver, forceMergeDeletesHandler);
    }

    @Override
    public void custom(CustomRequest request, StreamObserver<CustomResponse> responseObserver) {
      Handler.handleUnaryRequest("custom", request, responseObserver, customHandler);
    }
  }

  static class ReplicationServerImpl extends ReplicationServerGrpc.ReplicationServerImplBase {
    private final AddReplicaHandler addReplicaHandler;
    private final CopyFilesHandler copyFilesHandler;
    private final GetNodesInfoHandler getNodesInfoHandler;
    private final NewNRTPointHandler newNRTPointHandler;
    private final RecvCopyStateHandler recvCopyStateHandler;
    private final RecvRawFileHandler recvRawFileHandler;
    private final RecvRawFileV2Handler recvRawFileV2Handler;
    private final ReplicaCurrentSearchingVersionHandler replicaCurrentSearchingVersionHandler;
    private final SendRawFileHandler sendRawFileHandler;
    private final WriteNRTPointHandler writeNRTPointHandler;

    public ReplicationServerImpl(GlobalState globalState, boolean verifyIndexId) {

      addReplicaHandler = new AddReplicaHandler(globalState, verifyIndexId);
      copyFilesHandler = new CopyFilesHandler(globalState, verifyIndexId);
      getNodesInfoHandler = new GetNodesInfoHandler(globalState);
      newNRTPointHandler = new NewNRTPointHandler(globalState, verifyIndexId);
      recvCopyStateHandler = new RecvCopyStateHandler(globalState, verifyIndexId);
      recvRawFileHandler = new RecvRawFileHandler(globalState, verifyIndexId);
      recvRawFileV2Handler = new RecvRawFileV2Handler(globalState, verifyIndexId);
      replicaCurrentSearchingVersionHandler =
          new ReplicaCurrentSearchingVersionHandler(globalState);
      sendRawFileHandler = new SendRawFileHandler(globalState);
      writeNRTPointHandler = new WriteNRTPointHandler(globalState);
    }

    @Override
    public void addReplicas(
        AddReplicaRequest addReplicaRequest,
        StreamObserver<AddReplicaResponse> responseStreamObserver) {
      Handler.handleUnaryRequest(
          "addReplicas", addReplicaRequest, responseStreamObserver, addReplicaHandler);
    }

    @Override
    public StreamObserver<RawFileChunk> sendRawFile(
        StreamObserver<TransferStatus> responseObserver) {
      return sendRawFileHandler.handle(responseObserver);
    }

    @Override
    public void recvRawFile(
        FileInfo fileInfoRequest, StreamObserver<RawFileChunk> rawFileChunkStreamObserver) {
      recvRawFileHandler.handle(fileInfoRequest, rawFileChunkStreamObserver);
    }

    @Override
    public StreamObserver<FileInfo> recvRawFileV2(
        StreamObserver<RawFileChunk> rawFileChunkStreamObserver) {
      return recvRawFileV2Handler.handle(rawFileChunkStreamObserver);
    }

    @Override
    public void recvCopyState(
        CopyStateRequest request, StreamObserver<CopyState> responseObserver) {
      Handler.handleUnaryRequest("recvCopyState", request, responseObserver, recvCopyStateHandler);
    }

    @Override
    public void copyFiles(CopyFiles request, StreamObserver<TransferStatus> responseObserver) {
      copyFilesHandler.handle(request, responseObserver);
    }

    @Override
    public void newNRTPoint(NewNRTPoint request, StreamObserver<TransferStatus> responseObserver) {
      Handler.handleUnaryRequest("newNRTPoint", request, responseObserver, newNRTPointHandler);
    }

    @Override
    public void writeNRTPoint(
        IndexName indexNameRequest, StreamObserver<SearcherVersion> responseObserver) {
      Handler.handleUnaryRequest(
          "writeNRTPoint", indexNameRequest, responseObserver, writeNRTPointHandler);
    }

    @Override
    public void getCurrentSearcherVersion(
        IndexName indexNameRequest, StreamObserver<SearcherVersion> responseObserver) {
      Handler.handleUnaryRequest(
          "getCurrentSearcherVersion",
          indexNameRequest,
          responseObserver,
          replicaCurrentSearchingVersionHandler);
    }

    @Override
    public void getConnectedNodes(
        GetNodesRequest getNodesRequest, StreamObserver<GetNodesResponse> responseObserver) {
      Handler.handleUnaryRequest(
          "getConnectedNodes", getNodesRequest, responseObserver, getNodesInfoHandler);
    }
  }
}
