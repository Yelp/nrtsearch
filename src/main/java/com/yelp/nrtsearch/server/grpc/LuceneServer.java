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
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.LuceneServerModule;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.config.QueryCacheConfig;
import com.yelp.nrtsearch.server.luceneserver.AddDocumentHandler;
import com.yelp.nrtsearch.server.luceneserver.AddReplicaHandler;
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
import com.yelp.nrtsearch.server.luceneserver.ReleaseSnapshotHandler;
import com.yelp.nrtsearch.server.luceneserver.ReplicaCurrentSearchingVersionHandler;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.SettingsHandler;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.WriteNRTPointHandler;
import com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.luceneserver.custom.request.CustomRequestProcessor;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefCreator;
import com.yelp.nrtsearch.server.luceneserver.handler.BackupWarmingQueriesHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.CommitHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.CreateIndexHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.CustomHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.ForceMergeDeletesHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.ForceMergeHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.GetAllSnapshotIndexGenHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.GlobalStateHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.IndicesHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.MetricsHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.ReadyHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.RefreshHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.RegisterFieldsHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.ReloadStateHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.SearchV2Handler;
import com.yelp.nrtsearch.server.luceneserver.handler.StartIndexHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.StartIndexV2Handler;
import com.yelp.nrtsearch.server.luceneserver.handler.StatsHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.StatusHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.StopIndexHandler;
import com.yelp.nrtsearch.server.luceneserver.handler.UpdateFieldsHandler;
import com.yelp.nrtsearch.server.luceneserver.highlights.HighlighterService;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import com.yelp.nrtsearch.server.luceneserver.index.handlers.LiveSettingsV2Handler;
import com.yelp.nrtsearch.server.luceneserver.index.handlers.SettingsV2Handler;
import com.yelp.nrtsearch.server.luceneserver.logging.HitsLoggerCreator;
import com.yelp.nrtsearch.server.luceneserver.rescore.RescorerCreator;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTaskCreator;
import com.yelp.nrtsearch.server.luceneserver.search.cache.NrtQueryCache;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreator;
import com.yelp.nrtsearch.server.luceneserver.similarity.SimilarityCreator;
import com.yelp.nrtsearch.server.monitoring.Configuration;
import com.yelp.nrtsearch.server.monitoring.DeadlineMetrics;
import com.yelp.nrtsearch.server.monitoring.DirSizeCollector;
import com.yelp.nrtsearch.server.monitoring.IndexMetrics;
import com.yelp.nrtsearch.server.monitoring.LuceneServerMonitoringServerInterceptor;
import com.yelp.nrtsearch.server.monitoring.MergeSchedulerCollector;
import com.yelp.nrtsearch.server.monitoring.NrtMetrics;
import com.yelp.nrtsearch.server.monitoring.ProcStatCollector;
import com.yelp.nrtsearch.server.monitoring.QueryCacheCollector;
import com.yelp.nrtsearch.server.monitoring.SearchResponseCollector;
import com.yelp.nrtsearch.server.monitoring.ThreadPoolCollector;
import com.yelp.nrtsearch.server.monitoring.ThreadPoolCollector.RejectionCounterWrapper;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.PluginsService;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.utils.ThreadPoolExecutorFactory;
import com.yelp.nrtsearch.tools.cli.VersionProvider;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.suggest.document.CompletionPostingsFormatUtil;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/** Server that manages startup/shutdown of a {@code LuceneServer} server. */
public class LuceneServer {
  private static final Logger logger = LoggerFactory.getLogger(LuceneServer.class.getName());
  private final RemoteBackend remoteBackend;
  private final PrometheusRegistry prometheusRegistry;
  private final PluginsService pluginsService;

  private Server server;
  private Server replicationServer;
  private final LuceneServerConfiguration luceneServerConfiguration;

  @Inject
  public LuceneServer(
      LuceneServerConfiguration luceneServerConfiguration,
      RemoteBackend remoteBackend,
      PrometheusRegistry prometheusRegistry) {
    this.luceneServerConfiguration = luceneServerConfiguration;
    this.remoteBackend = remoteBackend;
    this.prometheusRegistry = prometheusRegistry;
    this.pluginsService =
        new PluginsService(luceneServerConfiguration, remoteBackend, prometheusRegistry);
  }

  @VisibleForTesting
  public void start() throws IOException {
    List<Plugin> plugins = pluginsService.loadPlugins();

    LuceneServerImpl serverImpl =
        new LuceneServerImpl(luceneServerConfiguration, remoteBackend, prometheusRegistry, plugins);
    GlobalState globalState = serverImpl.getGlobalState();

    registerMetrics(globalState);

    if (luceneServerConfiguration.getMaxConcurrentCallsPerConnectionForReplication() != -1) {
      replicationServer =
          NettyServerBuilder.forPort(luceneServerConfiguration.getReplicationPort())
              .addService(
                  new ReplicationServerImpl(
                      globalState, luceneServerConfiguration.getVerifyReplicationIndexId()))
              .executor(
                  ThreadPoolExecutorFactory.getInstance()
                      .getThreadPoolExecutor(
                          ThreadPoolExecutorFactory.ExecutorType.REPLICATIONSERVER))
              .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
              .maxConcurrentCallsPerConnection(
                  luceneServerConfiguration.getMaxConcurrentCallsPerConnectionForReplication())
              .maxConnectionAge(
                  luceneServerConfiguration.getMaxConnectionAgeForReplication(), TimeUnit.SECONDS)
              .maxConnectionAgeGrace(
                  luceneServerConfiguration.getMaxConnectionAgeGraceForReplication(),
                  TimeUnit.SECONDS)
              .build()
              .start();
    } else {
      replicationServer =
          ServerBuilder.forPort(luceneServerConfiguration.getReplicationPort())
              .addService(
                  new ReplicationServerImpl(
                      globalState, luceneServerConfiguration.getVerifyReplicationIndexId()))
              .executor(
                  ThreadPoolExecutorFactory.getInstance()
                      .getThreadPoolExecutor(
                          ThreadPoolExecutorFactory.ExecutorType.REPLICATIONSERVER))
              .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
              .build()
              .start();
    }
    logger.info(
        "Server started, listening on "
            + luceneServerConfiguration.getReplicationPort()
            + " for replication messages");

    // Inform global state that the replication server is started, and it is safe to start indices
    globalState.replicationStarted(replicationServer.getPort());

    LuceneServerMonitoringServerInterceptor monitoringInterceptor =
        LuceneServerMonitoringServerInterceptor.create(
            Configuration.allMetrics()
                .withLatencyBuckets(luceneServerConfiguration.getMetricsBuckets())
                .withPrometheusRegistry(prometheusRegistry));
    /* The port on which the server should run */
    GrpcServerExecutorSupplier executorSupplier = new GrpcServerExecutorSupplier();
    server =
        ServerBuilder.forPort(luceneServerConfiguration.getPort())
            .addService(ServerInterceptors.intercept(serverImpl, monitoringInterceptor))
            .addService(ProtoReflectionService.newInstance())
            // Set executor supplier to use different thread pool for metrics method
            .callExecutor(executorSupplier)
            // We still need this executor to run tasks before the point when executorSupplier can
            // be called (https://github.com/grpc/grpc-java/issues/8274)
            .executor(executorSupplier.getGrpcThreadPoolExecutor())
            .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
            .compressorRegistry(LuceneServerStubBuilder.COMPRESSOR_REGISTRY)
            .decompressorRegistry(LuceneServerStubBuilder.DECOMPRESSOR_REGISTRY)
            .build()
            .start();
    logger.info(
        "Server started, listening on " + luceneServerConfiguration.getPort() + " for messages");
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
  }

  /** Main launches the server from the command line. */
  public static void main(String[] args) {
    System.exit(new CommandLine(new LuceneServerCommand()).execute(args));
  }

  @CommandLine.Command(
      name = "lucene-server",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class,
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
      LuceneServer luceneServer;
      try {
        Injector injector = Guice.createInjector(new LuceneServerModule(this));
        luceneServer = injector.getInstance(LuceneServer.class);
        luceneServer.start();

        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(
                    () -> {
                      // Use stderr here since the logger may have been reset by its JVM shutdown
                      // hook.
                      logger.error("*** shutting down gRPC server since JVM is shutting down");
                      luceneServer.stop();
                      logger.error("*** server shut down");
                    }));
      } catch (Throwable t) {
        logger.error("Uncaught exception", t);
        throw t;
      }
      luceneServer.blockUntilShutdown();
      return 0;
    }
  }

  static class LuceneServerImpl extends LuceneServerGrpc.LuceneServerImplBase {
    private final JsonFormat.Printer protoMessagePrinter =
        JsonFormat.printer().omittingInsignificantWhitespace();
    private final GlobalState globalState;
    private final PrometheusRegistry prometheusRegistry;
    private final ThreadPoolExecutor searchThreadPoolExecutor;

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
        LuceneServerConfiguration configuration,
        RemoteBackend remoteBackend,
        PrometheusRegistry prometheusRegistry,
        List<Plugin> plugins)
        throws IOException {
      this.prometheusRegistry = prometheusRegistry;

      DeadlineUtils.setCancellationEnabled(configuration.getDeadlineCancellation());
      CompletionPostingsFormatUtil.setCompletionCodecLoadMode(
          configuration.getCompletionCodecLoadMode());
      ThreadPoolExecutorFactory.init(configuration.getThreadPoolConfiguration());

      initQueryCache(configuration);
      initExtendableComponents(configuration, plugins);

      this.globalState = GlobalState.createState(configuration, remoteBackend);
      this.searchThreadPoolExecutor = globalState.getSearchThreadPoolExecutor();

      initializeHandlers();
    }

    @VisibleForTesting
    static void initQueryCache(LuceneServerConfiguration configuration) {
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

    private void initExtendableComponents(
        LuceneServerConfiguration configuration, List<Plugin> plugins) {
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

    private void initializeHandlers() {
      AddDocumentHandler.initialize(globalState);
      BackupWarmingQueriesHandler.initialize(globalState);
      CommitHandler.initialize(globalState);
      CreateIndexHandler.initialize(globalState);
      CreateSnapshotHandler.initialize(globalState);
      CustomHandler.initialize(globalState);
      DeleteAllDocumentsHandler.initialize(globalState);
      DeleteByQueryHandler.initialize(globalState);
      DeleteDocumentsHandler.initialize(globalState);
      DeleteIndexHandler.initialize(globalState);
      ForceMergeDeletesHandler.initialize(globalState);
      ForceMergeHandler.initialize(globalState);
      GetAllSnapshotIndexGenHandler.initialize(globalState);
      GetStateHandler.initialize(globalState);
      GlobalStateHandler.initialize(globalState);
      IndicesHandler.initialize(globalState);
      LiveSettingsHandler.initialize(globalState);
      LiveSettingsV2Handler.initialize(globalState);
      MetricsHandler.initialize(globalState, prometheusRegistry);
      ReadyHandler.initialize(globalState);
      RefreshHandler.initialize(globalState);
      RegisterFieldsHandler.initialize(globalState);
      ReleaseSnapshotHandler.initialize(globalState);
      ReloadStateHandler.initialize(globalState);
      SearchHandler.initialize(globalState, searchThreadPoolExecutor);
      SearchV2Handler.initialize(globalState);
      SettingsHandler.initialize(globalState);
      SettingsV2Handler.initialize(globalState);
      StartIndexHandler.initialize(globalState);
      StartIndexV2Handler.initialize(globalState);
      StatsHandler.initialize(globalState);
      StatusHandler.initialize(globalState);
      StopIndexHandler.initialize(globalState);
      UpdateFieldsHandler.initialize(globalState);
    }

    /** Get the global cluster state. */
    public GlobalState getGlobalState() {
      return globalState;
    }

    @Override
    public void createIndex(
        CreateIndexRequest req, StreamObserver<CreateIndexResponse> responseObserver) {
      CreateIndexHandler.getInstance().handle(req, responseObserver);
    }

    @Override
    public void liveSettings(
        LiveSettingsRequest req, StreamObserver<LiveSettingsResponse> responseObserver) {
      LiveSettingsHandler.getInstance().handle(req, responseObserver);
    }

    @Override
    public void liveSettingsV2(
        LiveSettingsV2Request req, StreamObserver<LiveSettingsV2Response> responseObserver) {
      LiveSettingsV2Handler.getInstance().handle(req, responseObserver);
    }

    @Override
    public void registerFields(
        FieldDefRequest fieldDefRequest, StreamObserver<FieldDefResponse> responseObserver) {
      RegisterFieldsHandler.getInstance().handle(fieldDefRequest, responseObserver);
    }

    @Override
    public void updateFields(
        FieldDefRequest fieldDefRequest, StreamObserver<FieldDefResponse> responseObserver) {
      UpdateFieldsHandler.getInstance().handle(fieldDefRequest, responseObserver);
    }

    @Override
    public void settings(
        SettingsRequest settingsRequest, StreamObserver<SettingsResponse> responseObserver) {
      SettingsHandler.getInstance().handle(settingsRequest, responseObserver);
    }

    @Override
    public void settingsV2(
        SettingsV2Request settingsRequest, StreamObserver<SettingsV2Response> responseObserver) {
      SettingsV2Handler.getInstance().handle(settingsRequest, responseObserver);
    }

    @Override
    public void startIndex(
        StartIndexRequest startIndexRequest, StreamObserver<StartIndexResponse> responseObserver) {
      StartIndexHandler.getInstance().handle(startIndexRequest, responseObserver);
    }

    @Override
    public void startIndexV2(
        StartIndexV2Request startIndexRequest,
        StreamObserver<StartIndexResponse> responseObserver) {
      StartIndexV2Handler.getInstance().handle(startIndexRequest, responseObserver);
    }

    @Override
    public StreamObserver<AddDocumentRequest> addDocuments(
        StreamObserver<AddDocumentResponse> responseObserver) {
      return AddDocumentHandler.getInstance().handle(responseObserver);
    }

    @Override
    public void refresh(
        RefreshRequest refreshRequest,
        StreamObserver<RefreshResponse> refreshResponseStreamObserver) {
      RefreshHandler.getInstance().handle(refreshRequest, refreshResponseStreamObserver);
    }

    @Override
    public void commit(
        CommitRequest commitRequest, StreamObserver<CommitResponse> commitResponseStreamObserver) {
      CommitHandler.getInstance().handle(commitRequest, commitResponseStreamObserver);
    }

    @Override
    public void stats(
        StatsRequest statsRequest, StreamObserver<StatsResponse> statsResponseStreamObserver) {
      StatsHandler.getInstance().handle(statsRequest, statsResponseStreamObserver);
    }

    @Override
    public void search(
        SearchRequest searchRequest, StreamObserver<SearchResponse> responseObserver) {
      SearchHandler.getInstance().handle(searchRequest, responseObserver);
    }

    @Override
    public void searchV2(
        SearchRequest searchRequest, StreamObserver<Any> searchResponseStreamObserver) {
      SearchV2Handler.getInstance().handle(searchRequest, searchResponseStreamObserver);
    }

    @Override
    public void delete(
        AddDocumentRequest addDocumentRequest,
        StreamObserver<AddDocumentResponse> responseObserver) {
      DeleteDocumentsHandler.getInstance().handle(addDocumentRequest, responseObserver);
    }

    @Override
    public void deleteByQuery(
        DeleteByQueryRequest deleteByQueryRequest,
        StreamObserver<AddDocumentResponse> responseObserver) {
      DeleteByQueryHandler.getInstance().handle(deleteByQueryRequest, responseObserver);
    }

    @Override
    public void deleteAll(
        DeleteAllDocumentsRequest deleteAllDocumentsRequest,
        StreamObserver<DeleteAllDocumentsResponse> responseObserver) {
      DeleteAllDocumentsHandler.getInstance().handle(deleteAllDocumentsRequest, responseObserver);
    }

    @Override
    public void deleteIndex(
        DeleteIndexRequest deleteIndexRequest,
        StreamObserver<DeleteIndexResponse> responseObserver) {
      DeleteIndexHandler.getInstance().handle(deleteIndexRequest, responseObserver);
    }

    @Override
    public void stopIndex(
        StopIndexRequest stopIndexRequest, StreamObserver<DummyResponse> responseObserver) {
      StopIndexHandler.getInstance().handle(stopIndexRequest, responseObserver);
    }

    @Override
    public void reloadState(
        ReloadStateRequest request, StreamObserver<ReloadStateResponse> responseObserver) {
      ReloadStateHandler.getInstance().handle(request, responseObserver);
    }

    @Override
    public void status(
        HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
      StatusHandler.getInstance().handle(request, responseObserver);
    }

    /**
     * Returns a valid response only if all indices in {@link GlobalState} are started or if any
     * index names are provided in {@link ReadyCheckRequest} returns a valid response if those
     * specific indices are started.
     */
    @Override
    public void ready(
        ReadyCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
      ReadyHandler.getInstance().handle(request, responseObserver);
    }

    @Override
    public void createSnapshot(
        CreateSnapshotRequest createSnapshotRequest,
        StreamObserver<CreateSnapshotResponse> responseObserver) {
      CreateSnapshotHandler.getInstance().handle(createSnapshotRequest, responseObserver);
    }

    @Override
    public void releaseSnapshot(
        ReleaseSnapshotRequest releaseSnapshotRequest,
        StreamObserver<ReleaseSnapshotResponse> responseObserver) {
      ReleaseSnapshotHandler.getInstance().handle(releaseSnapshotRequest, responseObserver);
    }

    @Override
    public void getAllSnapshotIndexGen(
        GetAllSnapshotGenRequest request,
        StreamObserver<GetAllSnapshotGenResponse> responseObserver) {
      GetAllSnapshotIndexGenHandler.getInstance().handle(request, responseObserver);
    }

    @Override
    public void backupWarmingQueries(
        BackupWarmingQueriesRequest request,
        StreamObserver<BackupWarmingQueriesResponse> responseObserver) {
      BackupWarmingQueriesHandler.getInstance().handle(request, responseObserver);
    }

    @Override
    public void metrics(Empty request, StreamObserver<HttpBody> responseObserver) {
      MetricsHandler.getInstance().handle(request, responseObserver);
    }

    @Override
    public void indices(IndicesRequest request, StreamObserver<IndicesResponse> responseObserver) {
      IndicesHandler.getInstance().handle(request, responseObserver);
    }

    @Override
    public void globalState(
        GlobalStateRequest request, StreamObserver<GlobalStateResponse> responseObserver) {
      GlobalStateHandler.getInstance().handle(request, responseObserver);
    }

    @Override
    public void state(StateRequest request, StreamObserver<StateResponse> responseObserver) {
      GetStateHandler.getInstance().handle(request, responseObserver);
    }

    @Override
    public void forceMerge(
        ForceMergeRequest forceMergeRequest, StreamObserver<ForceMergeResponse> responseObserver) {
      ForceMergeHandler.getInstance().handle(forceMergeRequest, responseObserver);
    }

    @Override
    public void forceMergeDeletes(
        ForceMergeDeletesRequest forceMergeRequest,
        StreamObserver<ForceMergeDeletesResponse> responseObserver) {
      ForceMergeDeletesHandler.getInstance().handle(forceMergeRequest, responseObserver);
    }

    @Override
    public void custom(CustomRequest request, StreamObserver<CustomResponse> responseObserver) {
      CustomHandler.getInstance().handle(request, responseObserver);
    }
  }

  static class ReplicationServerImpl extends ReplicationServerGrpc.ReplicationServerImplBase {
    private final GlobalState globalState;
    private final boolean verifyIndexId;

    @VisibleForTesting
    static void checkIndexId(String actual, String expected, boolean throwException) {
      if (!actual.equals(expected)) {
        String message =
            String.format("Index id mismatch, expected: %s, actual: %s", expected, actual);
        if (throwException) {
          throw Status.FAILED_PRECONDITION.withDescription(message).asRuntimeException();
        } else {
          logger.warn(message);
        }
      }
    }

    public ReplicationServerImpl(GlobalState globalState, boolean verifyIndexId) {
      this.globalState = globalState;
      this.verifyIndexId = verifyIndexId;
    }

    @Override
    public void addReplicas(
        AddReplicaRequest addReplicaRequest,
        StreamObserver<AddReplicaResponse> responseStreamObserver) {
      try {
        IndexStateManager indexStateManager =
            globalState.getIndexStateManager(addReplicaRequest.getIndexName());
        checkIndexId(addReplicaRequest.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

        IndexState indexState = indexStateManager.getCurrent();
        boolean useKeepAliveForReplication =
            globalState.getConfiguration().getUseKeepAliveForReplication();
        AddReplicaResponse reply =
            new AddReplicaHandler(useKeepAliveForReplication).handle(indexState, addReplicaRequest);
        logger.info("AddReplicaHandler returned " + reply.toString());
        responseStreamObserver.onNext(reply);
        responseStreamObserver.onCompleted();
      } catch (StatusRuntimeException e) {
        logger.warn("error while trying addReplicas " + addReplicaRequest.getIndexName(), e);
        responseStreamObserver.onError(e);
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
        IndexStateManager indexStateManager =
            globalState.getIndexStateManager(fileInfoRequest.getIndexName());
        checkIndexId(fileInfoRequest.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

        IndexState indexState = indexStateManager.getCurrent();
        ShardState shardState = indexState.getShard(0);
        try (IndexInput luceneFile =
            shardState.indexDir.openInput(fileInfoRequest.getFileName(), IOContext.DEFAULT)) {
          long len = luceneFile.length();
          long pos = fileInfoRequest.getFpStart();
          luceneFile.seek(pos);
          byte[] buffer = new byte[1024 * 64];
          long totalRead;
          totalRead = pos;
          while (totalRead < len) {
            int chunkSize = (int) Math.min(buffer.length, (len - totalRead));
            luceneFile.readBytes(buffer, 0, chunkSize);
            RawFileChunk rawFileChunk =
                RawFileChunk.newBuilder()
                    .setContent(ByteString.copyFrom(buffer, 0, chunkSize))
                    .build();
            rawFileChunkStreamObserver.onNext(rawFileChunk);
            totalRead += chunkSize;
          }
          // EOF
          rawFileChunkStreamObserver.onCompleted();
        }
      } catch (StatusRuntimeException e) {
        logger.warn("error on recvRawFile " + fileInfoRequest.getFileName(), e);
        rawFileChunkStreamObserver.onError(e);
      } catch (Exception e) {
        logger.warn("error on recvRawFile " + fileInfoRequest.getFileName(), e);
        rawFileChunkStreamObserver.onError(
            Status.INTERNAL
                .withDescription("error on recvRawFile: " + fileInfoRequest.getFileName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public StreamObserver<FileInfo> recvRawFileV2(
        StreamObserver<RawFileChunk> rawFileChunkStreamObserver) {
      return new StreamObserver<>() {
        private IndexState indexState;
        private IndexInput luceneFile;
        private byte[] buffer;
        private final int ackEvery =
            globalState.getConfiguration().getFileCopyConfig().getAckEvery();
        private final int maxInflight =
            globalState.getConfiguration().getFileCopyConfig().getMaxInFlight();
        private int lastAckedSeq = 0;
        private int currentSeq = 0;
        private long fileOffset;
        private long fileLength;

        @Override
        public void onNext(FileInfo fileInfoRequest) {
          try {
            if (indexState == null) {
              // Start transfer
              IndexStateManager indexStateManager =
                  globalState.getIndexStateManager(fileInfoRequest.getIndexName());
              checkIndexId(
                  fileInfoRequest.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

              indexState = indexStateManager.getCurrent();
              ShardState shardState = indexState.getShard(0);
              if (shardState == null) {
                throw new IllegalStateException(
                    "Error getting shard state for: " + fileInfoRequest.getIndexName());
              }
              luceneFile =
                  shardState.indexDir.openInput(fileInfoRequest.getFileName(), IOContext.DEFAULT);
              luceneFile.seek(fileInfoRequest.getFpStart());
              fileOffset = fileInfoRequest.getFpStart();
              fileLength = luceneFile.length();
              buffer = new byte[globalState.getConfiguration().getFileCopyConfig().getChunkSize()];
            } else {
              // ack existing transfer
              lastAckedSeq = fileInfoRequest.getAckSeqNum();
              if (lastAckedSeq <= 0) {
                throw new IllegalArgumentException(
                    "Invalid ackSeqNum: " + fileInfoRequest.getAckSeqNum());
              }
            }
            while (fileOffset < fileLength && (currentSeq - lastAckedSeq) < maxInflight) {
              int chunkSize = (int) Math.min(buffer.length, (fileLength - fileOffset));
              luceneFile.readBytes(buffer, 0, chunkSize);
              currentSeq++;
              RawFileChunk rawFileChunk =
                  RawFileChunk.newBuilder()
                      .setContent(ByteString.copyFrom(buffer, 0, chunkSize))
                      .setSeqNum(currentSeq)
                      .setAck((currentSeq % ackEvery) == 0)
                      .build();
              rawFileChunkStreamObserver.onNext(rawFileChunk);
              fileOffset += chunkSize;
              if (fileOffset == fileLength) {
                rawFileChunkStreamObserver.onCompleted();
              }
            }
            logger.debug(
                String.format("recvRawFileV2: in flight chunks: %d", currentSeq - lastAckedSeq));
          } catch (Throwable t) {
            maybeCloseFile();
            rawFileChunkStreamObserver.onError(t);
            throw new RuntimeException(t);
          }
        }

        @Override
        public void onError(Throwable t) {
          logger.error("recvRawFileV2 onError", t);
          maybeCloseFile();
          rawFileChunkStreamObserver.onError(t);
        }

        @Override
        public void onCompleted() {
          maybeCloseFile();
          logger.debug("recvRawFileV2 onCompleted");
        }

        private void maybeCloseFile() {
          if (luceneFile != null) {
            try {
              luceneFile.close();
            } catch (IOException e) {
              logger.warn("Error closing index file", e);
            }
            luceneFile = null;
          }
        }
      };
    }

    @Override
    public void recvCopyState(
        CopyStateRequest request, StreamObserver<CopyState> responseObserver) {
      try {
        IndexStateManager indexStateManager =
            globalState.getIndexStateManager(request.getIndexName());
        checkIndexId(request.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

        IndexState indexState = indexStateManager.getCurrent();
        CopyState reply = new RecvCopyStateHandler().handle(indexState, request);
        logger.debug(
            "RecvCopyStateHandler returned, completedMergeFiles count: "
                + reply.getCompletedMergeFilesCount());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (StatusRuntimeException e) {
        logger.warn("error while trying recvCopyState " + request.getIndexName(), e);
        responseObserver.onError(e);
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
        IndexStateManager indexStateManager =
            globalState.getIndexStateManager(request.getIndexName());
        checkIndexId(request.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

        IndexState indexState = indexStateManager.getCurrent();
        CopyFilesHandler copyFilesHandler = new CopyFilesHandler();
        // we need to send multiple responses to client from this method
        copyFilesHandler.handle(indexState, request, responseObserver);
        logger.info("CopyFilesHandler returned successfully");
      } catch (StatusRuntimeException e) {
        logger.warn("error while trying copyFiles " + request.getIndexName(), e);
        responseObserver.onError(e);
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
        IndexStateManager indexStateManager =
            globalState.getIndexStateManager(request.getIndexName());
        checkIndexId(request.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

        IndexState indexState = indexStateManager.getCurrent();
        NewNRTPointHandler newNRTPointHander = new NewNRTPointHandler();
        TransferStatus reply = newNRTPointHander.handle(indexState, request);
        logger.debug(
            "NewNRTPointHandler returned status "
                + reply.getCode()
                + " message: "
                + reply.getMessage());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (StatusRuntimeException e) {
        logger.warn(
            String.format(
                "error on newNRTPoint for indexName: %s, for version: %s, primaryGen: %s",
                request.getIndexName(), request.getVersion(), request.getPrimaryGen()),
            e);
        responseObserver.onError(e);
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
        IndexStateManager indexStateManager =
            globalState.getIndexStateManager(indexNameRequest.getIndexName());
        IndexState indexState = indexStateManager.getCurrent();
        WriteNRTPointHandler writeNRTPointHander =
            new WriteNRTPointHandler(indexStateManager.getIndexId());
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
