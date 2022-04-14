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
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.LuceneServerModule;
import com.yelp.nrtsearch.server.MetricsRequestHandler;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.config.QueryCacheConfig;
import com.yelp.nrtsearch.server.luceneserver.*;
import com.yelp.nrtsearch.server.luceneserver.AddDocumentHandler.DocumentIndexer;
import com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefCreator;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import com.yelp.nrtsearch.server.luceneserver.index.handlers.FieldUpdateHandler;
import com.yelp.nrtsearch.server.luceneserver.index.handlers.LiveSettingsV2Handler;
import com.yelp.nrtsearch.server.luceneserver.index.handlers.SettingsV2Handler;
import com.yelp.nrtsearch.server.luceneserver.rescore.RescorerCreator;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTaskCreator;
import com.yelp.nrtsearch.server.luceneserver.search.cache.NrtQueryCache;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreator;
import com.yelp.nrtsearch.server.luceneserver.similarity.SimilarityCreator;
import com.yelp.nrtsearch.server.luceneserver.warming.Warmer;
import com.yelp.nrtsearch.server.monitoring.*;
import com.yelp.nrtsearch.server.monitoring.ThreadPoolCollector.RejectionCounterWrapper;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.PluginsService;
import com.yelp.nrtsearch.server.utils.ThreadPoolExecutorFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/** Server that manages startup/shutdown of a {@code LuceneServer} server. */
public class LuceneServer {
  private static final Logger logger = LoggerFactory.getLogger(LuceneServer.class.getName());
  private static final Splitter COMMA_SPLITTER = Splitter.on(",");
  private final Archiver archiver;
  private final Archiver incArchiver;
  private final CollectorRegistry collectorRegistry;
  private final PluginsService pluginsService;

  private Server server;
  private Server replicationServer;
  private final LuceneServerConfiguration luceneServerConfiguration;

  @Inject
  public LuceneServer(
      LuceneServerConfiguration luceneServerConfiguration,
      @Named("legacyArchiver") Archiver archiver,
      @Named("incArchiver") Archiver incArchiver,
      CollectorRegistry collectorRegistry) {
    this.luceneServerConfiguration = luceneServerConfiguration;
    this.archiver = archiver;
    this.incArchiver = incArchiver;
    this.collectorRegistry = collectorRegistry;
    this.pluginsService = new PluginsService(luceneServerConfiguration);
  }

  private void start() throws IOException {
    List<Plugin> plugins = pluginsService.loadPlugins();
    String serviceName = luceneServerConfiguration.getServiceName();
    String nodeName = luceneServerConfiguration.getNodeName();

    GlobalState globalState;
    LuceneServerImpl serverImpl;

    // For legacy index state, the GlobalState is created first and passed into
    // the LuceneServerImpl. For the newer state handling, the GlobalState loads the
    // IndexState, and must be created after the extendable components are initialized
    if (luceneServerConfiguration.getStateConfig().useLegacyStateManagement()) {
      globalState = GlobalState.createState(luceneServerConfiguration, incArchiver);

      // Only do state restore if using legacy state management.
      // Otherwise, this will be done during GlobalState initialization.
      if (luceneServerConfiguration.getRestoreState()) {
        logger.info("Loading state for any previously backed up indexes");
        List<String> indexes =
            RestoreStateHandler.restore(
                archiver,
                incArchiver,
                globalState,
                luceneServerConfiguration.getServiceName(),
                luceneServerConfiguration.getRestoreFromIncArchiver());
        for (String index : indexes) {
          logger.info("Loaded state for index " + index);
        }
      }

      serverImpl =
          new LuceneServerImpl(
              globalState,
              luceneServerConfiguration,
              archiver,
              incArchiver,
              collectorRegistry,
              plugins);
    } else {
      serverImpl =
          new LuceneServerImpl(
              luceneServerConfiguration, archiver, incArchiver, collectorRegistry, plugins);
      globalState = serverImpl.getGlobalState();
    }

    registerMetrics(globalState);

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
            .addService(ServerInterceptors.intercept(serverImpl, monitoringInterceptor))
            .addService(ProtoReflectionService.newInstance())
            .executor(
                ThreadPoolExecutorFactory.getThreadPoolExecutor(
                    ThreadPoolExecutorFactory.ExecutorType.LUCENESERVER,
                    luceneServerConfiguration.getThreadPoolConfiguration()))
            .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
            .compressorRegistry(LuceneServerStubBuilder.COMPRESSOR_REGISTRY)
            .decompressorRegistry(LuceneServerStubBuilder.DECOMPRESSOR_REGISTRY)
            .build()
            .start();
    logger.info(
        "Server started, listening on " + luceneServerConfiguration.getPort() + " for messages");

    if (luceneServerConfiguration.getMaxConcurrentCallsPerConnectionForReplication() != -1) {
      replicationServer =
          NettyServerBuilder.forPort(luceneServerConfiguration.getReplicationPort())
              .addService(new ReplicationServerImpl(globalState))
              .executor(
                  ThreadPoolExecutorFactory.getThreadPoolExecutor(
                      ThreadPoolExecutorFactory.ExecutorType.REPLICATIONSERVER,
                      luceneServerConfiguration.getThreadPoolConfiguration()))
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
              .addService(new ReplicationServerImpl(globalState))
              .executor(
                  ThreadPoolExecutorFactory.getThreadPoolExecutor(
                      ThreadPoolExecutorFactory.ExecutorType.REPLICATIONSERVER,
                      luceneServerConfiguration.getThreadPoolConfiguration()))
              .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
              .build()
              .start();
    }

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

  /** Register prometheus metrics exposed by /status/metrics */
  private void registerMetrics(GlobalState globalState) {
    // register jvm metrics
    if (luceneServerConfiguration.getPublishJvmMetrics()) {
      DefaultExports.register(collectorRegistry);
    }
    // register thread pool metrics
    new ThreadPoolCollector().register(collectorRegistry);
    collectorRegistry.register(RejectionCounterWrapper.rejectionCounter);
    // register nrt metrics
    NrtMetrics.register(collectorRegistry);
    // register index metrics
    IndexMetrics.register(collectorRegistry);
    // register query cache metrics
    new QueryCacheCollector().register(collectorRegistry);
    // register deadline cancellation metrics
    DeadlineMetrics.register(collectorRegistry);
    // register directory size metrics
    new DirSizeCollector(globalState).register(collectorRegistry);
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
      LuceneServer luceneServer;
      try {
        Injector injector = Guice.createInjector(new LuceneServerModule(this));
        luceneServer = injector.getInstance(LuceneServer.class);
        luceneServer.start();
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
    private final Archiver archiver;
    private final Archiver incArchiver;
    private final CollectorRegistry collectorRegistry;
    private final ThreadPoolExecutor searchThreadPoolExecutor;
    private final String archiveDirectory;
    private final boolean backupFromIncArchiver;
    private final boolean restoreFromIncArchiver;

    LuceneServerImpl(
        GlobalState globalState,
        LuceneServerConfiguration configuration,
        Archiver archiver,
        Archiver incArchiver,
        CollectorRegistry collectorRegistry,
        List<Plugin> plugins) {
      this.globalState = globalState;
      this.archiver = archiver;
      this.incArchiver = incArchiver;
      this.archiveDirectory = configuration.getArchiveDirectory();
      this.collectorRegistry = collectorRegistry;
      this.searchThreadPoolExecutor = globalState.getSearchThreadPoolExecutor();
      this.backupFromIncArchiver = configuration.getBackupWithInArchiver();
      this.restoreFromIncArchiver = configuration.getRestoreFromIncArchiver();

      DeadlineUtils.setCancellationEnabled(configuration.getDeadlineCancellation());

      initQueryCache(configuration);
      initExtendableComponents(configuration, plugins);
    }

    /**
     * Constructor used with newer state handling. Defers initialization of global state until after
     * extendable components.
     *
     * @param configuration server configuration
     * @param archiver archiver for external file transfer
     * @param incArchiver archiver for incremental index data copy
     * @param collectorRegistry metrics collector registry
     * @param plugins loaded plugins
     * @throws IOException
     */
    LuceneServerImpl(
        LuceneServerConfiguration configuration,
        Archiver archiver,
        Archiver incArchiver,
        CollectorRegistry collectorRegistry,
        List<Plugin> plugins)
        throws IOException {
      this.archiver = archiver;
      this.incArchiver = incArchiver;
      this.archiveDirectory = configuration.getArchiveDirectory();
      this.collectorRegistry = collectorRegistry;
      this.backupFromIncArchiver = configuration.getBackupWithInArchiver();
      this.restoreFromIncArchiver = configuration.getRestoreFromIncArchiver();

      DeadlineUtils.setCancellationEnabled(configuration.getDeadlineCancellation());

      initQueryCache(configuration);
      initExtendableComponents(configuration, plugins);

      this.globalState = GlobalState.createState(configuration, incArchiver);
      this.searchThreadPoolExecutor = globalState.getSearchThreadPoolExecutor();
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
      AnalyzerCreator.initialize(configuration, plugins);
      CollectorCreator.initialize(configuration, plugins);
      FetchTaskCreator.initialize(configuration, plugins);
      FieldDefCreator.initialize(configuration, plugins);
      RescorerCreator.initialize(configuration, plugins);
      ScriptService.initialize(configuration, plugins);
      SimilarityCreator.initialize(configuration, plugins);
    }

    /** Get the global cluster state. */
    public GlobalState getGlobalState() {
      return globalState;
    }

    /**
     * Set response compression on the provided {@link StreamObserver}. Should be a valid
     * compression type from the {@link LuceneServerStubBuilder#COMPRESSOR_REGISTRY}, or empty
     * string for default. Falls back to uncompressed on any error.
     *
     * @param compressionType compression type, or empty string
     * @param responseObserver observer to set compression on
     */
    private void setResponseCompression(
        String compressionType, StreamObserver<?> responseObserver) {
      if (!compressionType.isEmpty()) {
        try {
          ServerCallStreamObserver<?> serverCallStreamObserver =
              (ServerCallStreamObserver<?>) responseObserver;
          serverCallStreamObserver.setCompression(compressionType);
        } catch (Exception e) {
          logger.warn(
              "Unable to set response compression to type '" + compressionType + "' : " + e);
        }
      }
    }

    @Override
    public void createIndex(
        CreateIndexRequest req, StreamObserver<CreateIndexResponse> responseObserver) {
      String indexName = req.getIndexName();
      String validIndexNameRegex = "[A-z0-9_-]+";
      if (!indexName.matches(validIndexNameRegex)) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(
                    String.format(
                        "Index name %s is invalid - must contain only a-z, A-Z or 0-9", indexName))
                .asRuntimeException());
        return;
      }

      try {
        IndexState indexState = globalState.createIndex(indexName);
        // shards are initialized elsewhere for non-legacy state
        if (globalState.getConfiguration().getStateConfig().useLegacyStateManagement()) {
          // Create the first shard
          logger.info("NOW ADD SHARD 0");
          indexState.addShard(0, true);
          logger.info("DONE ADD SHARD 0");
        }
        String response = String.format("Created Index name: %s", indexName);
        CreateIndexResponse reply = CreateIndexResponse.newBuilder().setResponse(response).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (IllegalArgumentException e) {
        logger.warn("invalid IndexName: " + indexName, e);
        responseObserver.onError(
            Status.ALREADY_EXISTS
                .withDescription("invalid indexName: " + indexName)
                .augmentDescription("IllegalArgumentException()")
                .withCause(e)
                .asRuntimeException());
      } catch (Exception e) {
        logger.warn(
            "error while trying to save index state to disk for indexName: " + indexName, e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to save index state to disk for indexName: " + indexName)
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
    public void liveSettingsV2(
        LiveSettingsV2Request req, StreamObserver<LiveSettingsV2Response> responseObserver) {
      try {
        IndexStateManager indexStateManager = globalState.getIndexStateManager(req.getIndexName());
        LiveSettingsV2Response reply = LiveSettingsV2Handler.handle(indexStateManager, req);
        logger.info("LiveSettingsV2Handler returned " + JsonFormat.printer().print(reply));
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
            "error while trying to process live settings for indexName: " + req.getIndexName(), e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "error while trying to process live settings for indexName: "
                        + req.getIndexName())
                .augmentDescription("Exception()")
                .withCause(e)
                .asRuntimeException());
      }
    }

    @Override
    public void registerFields(
        FieldDefRequest fieldDefRequest, StreamObserver<FieldDefResponse> responseObserver) {
      try {
        FieldDefResponse reply;
        if (globalState.getConfiguration().getStateConfig().useLegacyStateManagement()) {
          IndexState indexState = globalState.getIndex(fieldDefRequest.getIndexName());
          reply = new RegisterFieldsHandler().handle(indexState, fieldDefRequest);
        } else {
          IndexStateManager indexStateManager =
              globalState.getIndexStateManager(fieldDefRequest.getIndexName());
          reply = FieldUpdateHandler.handle(indexStateManager, fieldDefRequest);
        }
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
        FieldDefResponse reply;
        if (globalState.getConfiguration().getStateConfig().useLegacyStateManagement()) {
          IndexState indexState = globalState.getIndex(fieldDefRequest.getIndexName());
          reply = new UpdateFieldsHandler().handle(indexState, fieldDefRequest);
        } else {
          IndexStateManager indexStateManager =
              globalState.getIndexStateManager(fieldDefRequest.getIndexName());
          reply = FieldUpdateHandler.handle(indexStateManager, fieldDefRequest);
        }
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
    public void settingsV2(
        SettingsV2Request settingsRequest, StreamObserver<SettingsV2Response> responseObserver) {
      try {
        IndexStateManager indexStateManager =
            globalState.getIndexStateManager(settingsRequest.getIndexName());
        SettingsV2Response reply = SettingsV2Handler.handle(indexStateManager, settingsRequest);
        logger.info("SettingsV2Handler returned: " + JsonFormat.printer().print(reply));
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
        StartIndexResponse reply;
        if (globalState.getConfiguration().getStateConfig().useLegacyStateManagement()) {
          IndexState indexState;
          StartIndexHandler startIndexHandler =
              new StartIndexHandler(
                  archiver,
                  incArchiver,
                  archiveDirectory,
                  backupFromIncArchiver,
                  restoreFromIncArchiver,
                  true,
                  null);

          indexState =
              globalState.getIndex(
                  startIndexRequest.getIndexName(), startIndexRequest.hasRestore());
          reply = startIndexHandler.handle(indexState, startIndexRequest);
        } else {
          reply = globalState.startIndex(startIndexRequest);
        }
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
                .augmentDescription(e.getMessage())
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
        Multimap<String, Future<Long>> futures = HashMultimap.create();
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
                      new DocumentIndexer(globalState, addDocRequestList, indexName));
              futures.put(indexName, future);
            } catch (Exception e) {
              responseObserver.onError(e);
            } finally {
              addDocumentRequestQueue.clear();
            }
          }
        }

        @Override
        public void onError(Throwable t) {
          logger.warn("addDocuments Cancelled", t);
          responseObserver.onError(t);
        }

        private String onCompletedForIndex(String indexName) {
          ArrayBlockingQueue<AddDocumentRequest> addDocumentRequestQueue =
              getAddDocumentRequestQueue(indexName);
          logger.debug(
              String.format(
                  "onCompleted, addDocumentRequestQueue: %s", addDocumentRequestQueue.size()));
          long highestGen = -1;
          try {
            // index the left over docs
            if (!addDocumentRequestQueue.isEmpty()) {
              logger.debug(
                  String.format(
                      "indexing left over addDocumentRequestQueue of size: %s",
                      addDocumentRequestQueue.size()));
              List<AddDocumentRequest> addDocRequestList = new ArrayList<>(addDocumentRequestQueue);
              // Since we are already running in the indexing threadpool run the indexing job
              // for remaining documents directly. This serializes indexing remaining documents for
              // multiple indices but avoids deadlocking if there aren't more threads than the
              // maximum
              // number of parallel addDocuments calls.
              long gen =
                  new DocumentIndexer(globalState, addDocRequestList, indexName).runIndexingJob();
              if (gen > highestGen) {
                highestGen = gen;
              }
            }
            // collect futures, block if needed
            int numIndexingChunks = futures.size();
            long t0 = System.nanoTime();
            for (Future<Long> result : futures.get(indexName)) {
              Long gen = result.get();
              logger.debug("Indexing returned sequence-number {}", gen);
              if (gen > highestGen) {
                highestGen = gen;
              }
            }
            long t1 = System.nanoTime();
            logger.debug(
                "Indexing job completed for {} docs, in {} chunks, with latest sequence number: {}, took: {} micro seconds",
                getCount(indexName),
                numIndexingChunks,
                highestGen,
                ((t1 - t0) / 1000));
            return String.valueOf(highestGen);
          } catch (Exception e) {
            logger.warn("error while trying to addDocuments", e);
            throw Status.INTERNAL
                .withDescription("error while trying to addDocuments ")
                .augmentDescription(e.getMessage())
                .withCause(e)
                .asRuntimeException();
          } finally {
            addDocumentRequestQueue.clear();
            countMap.put(indexName, 0L);
          }
        }

        @Override
        public void onCompleted() {
          try {
            globalState.submitIndexingTask(
                () -> {
                  try {
                    // TODO: this should return a map on index to genId in the response
                    String genId = "-1";
                    for (String indexName : addDocumentRequestQueueMap.keySet()) {
                      genId = onCompletedForIndex(indexName);
                    }
                    responseObserver.onNext(
                        AddDocumentResponse.newBuilder()
                            .setGenId(genId)
                            .setPrimaryId(globalState.getEphemeralId())
                            .build());
                    responseObserver.onCompleted();
                  } catch (Throwable t) {
                    responseObserver.onError(t);
                  }
                  return null;
                });
          } catch (RejectedExecutionException e) {
            logger.error("Threadpool is full, unable to submit indexing completion job");
            responseObserver.onError(
                Status.RESOURCE_EXHAUSTED
                    .withDescription("Threadpool is full, unable to submit indexing completion job")
                    .augmentDescription(e.getMessage())
                    .asRuntimeException());
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
        globalState.submitIndexingTask(
            () -> {
              try {
                IndexState indexState = globalState.getIndex(commitRequest.getIndexName());
                long gen = indexState.commit(backupFromIncArchiver);
                CommitResponse reply =
                    CommitResponse.newBuilder()
                        .setGen(gen)
                        .setPrimaryId(globalState.getEphemeralId())
                        .build();
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
                logger.warn(
                    "error while trying to commit to  index " + commitRequest.getIndexName(), e);
                commitResponseStreamObserver.onError(
                    Status.UNKNOWN
                        .withDescription(
                            "error while trying to commit to index: "
                                + commitRequest.getIndexName())
                        .augmentDescription(e.getMessage())
                        .asRuntimeException());
              }
              return null;
            });
      } catch (RejectedExecutionException e) {
        logger.error(
            "Threadpool is full, unable to submit commit to index {}",
            commitRequest.getIndexName());
        commitResponseStreamObserver.onError(
            Status.RESOURCE_EXHAUSTED
                .withDescription(
                    "Threadpool is full, unable to submit commit to index: "
                        + commitRequest.getIndexName())
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
        setResponseCompression(
            searchRequest.getResponseCompression(), searchResponseStreamObserver);
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
        String searchRequestJson = null;
        try {
          searchRequestJson = protoMessagePrinter.print(searchRequest);
        } catch (InvalidProtocolBufferException ignored) {
          // Ignore as invalid proto would have thrown an exception earlier
        }
        logger.warn(
            String.format(
                "error while trying to execute search for index %s: request: %s",
                searchRequest.getIndexName(), searchRequestJson),
            e);
        if (e instanceof StatusRuntimeException) {
          searchResponseStreamObserver.onError(e);
        } else {
          searchResponseStreamObserver.onError(
              Status.UNKNOWN
                  .withDescription(
                      String.format(
                          "error while trying to execute search for index %s. check logs for full searchRequest.",
                          searchRequest.getIndexName()))
                  .augmentDescription(e.getMessage())
                  .asRuntimeException());
        }
      }
    }

    @Override
    public void searchV2(
        SearchRequest searchRequest, StreamObserver<Any> searchResponseStreamObserver) {
      try {
        IndexState indexState = globalState.getIndex(searchRequest.getIndexName());
        setResponseCompression(
            searchRequest.getResponseCompression(), searchResponseStreamObserver);
        SearchHandler searchHandler = new SearchHandler(searchThreadPoolExecutor);
        SearchResponse reply = searchHandler.handle(indexState, searchRequest);
        searchResponseStreamObserver.onNext(Any.pack(reply));
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
        String searchRequestJson = null;
        try {
          searchRequestJson = protoMessagePrinter.print(searchRequest);
        } catch (InvalidProtocolBufferException ignored) {
          // Ignore as invalid proto would have thrown an exception earlier
        }
        logger.warn(
            String.format(
                "error while trying to execute search for index %s: request: %s",
                searchRequest.getIndexName(), searchRequestJson),
            e);
        if (e instanceof StatusRuntimeException) {
          searchResponseStreamObserver.onError(e);
        } else {
          searchResponseStreamObserver.onError(
              Status.UNKNOWN
                  .withDescription(
                      String.format(
                          "error while trying to execute search for index %s. check logs for full searchRequest.",
                          searchRequest.getIndexName()))
                  .augmentDescription(e.getMessage())
                  .asRuntimeException());
        }
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
        logger.info("DeleteIndexHandler returned " + reply.toString());
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
        DummyResponse reply;
        if (globalState.getConfiguration().getStateConfig().useLegacyStateManagement()) {
          IndexState indexState = globalState.getIndex(stopIndexRequest.getIndexName());
          reply = new StopIndexHandler().handle(indexState, stopIndexRequest);
        } else {
          reply = globalState.stopIndex(stopIndexRequest);
        }
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

    /**
     * Returns a valid response only if all indices in {@link GlobalState} are started or if any
     * index names are provided in {@link ReadyCheckRequest} returns a valid response if those
     * specific indices are started.
     */
    @Override
    public void ready(
        ReadyCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
      Set<String> indexNames;

      // If specific index names are provided we will check only those indices, otherwise check all
      if (request.getIndexNames().isEmpty()) {
        indexNames = globalState.getIndicesToStart();
      } else {
        List<String> indexNamesToCheck = COMMA_SPLITTER.splitToList(request.getIndexNames());

        Set<String> allIndices = globalState.getIndexNames();

        Sets.SetView<String> nonExistentIndices =
            Sets.difference(Set.copyOf(indexNamesToCheck), allIndices);
        if (!nonExistentIndices.isEmpty()) {
          logger.warn("Indices: {} do not exist", nonExistentIndices);
          responseObserver.onError(
              Status.UNAVAILABLE
                  .withDescription(String.format("Indices do not exist: %s", nonExistentIndices))
                  .asRuntimeException());
          return;
        }

        indexNames =
            allIndices.stream().filter(indexNamesToCheck::contains).collect(Collectors.toSet());
      }

      try {
        List<String> indicesNotStarted = new ArrayList<>();
        for (String indexName : indexNames) {
          IndexState indexState = globalState.getIndex(indexName);
          if (!indexState.isStarted()) {
            indicesNotStarted.add(indexName);
          }
        }

        if (indicesNotStarted.isEmpty()) {
          HealthCheckResponse reply =
              HealthCheckResponse.newBuilder().setHealth(TransferStatusCode.Done).build();
          logger.debug("Ready check returned " + reply.toString());
          responseObserver.onNext(reply);
          responseObserver.onCompleted();
        } else {
          logger.warn("Indices not started: {}", indicesNotStarted);
          responseObserver.onError(
              Status.UNAVAILABLE
                  .withDescription(String.format("Indices not started: %s", indicesNotStarted))
                  .asRuntimeException());
        }
      } catch (Exception e) {
        logger.warn("error while trying to check if all required indices are started", e);
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("error while trying to check if all required indices are started")
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
            new BackupIndexRequestHandler(
                archiver, incArchiver, archiveDirectory, backupFromIncArchiver);
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
    public void deleteIndexBackup(
        DeleteIndexBackupRequest request,
        StreamObserver<DeleteIndexBackupResponse> responseObserver) {
      try {
        IndexState indexState = globalState.getIndex(request.getIndexName());
        DeleteIndexBackupHandler backupIndexRequestHandler = new DeleteIndexBackupHandler(archiver);
        DeleteIndexBackupResponse reply = backupIndexRequestHandler.handle(indexState, request);
        logger.info("DeleteIndexBackupHandler returned results {}", reply.toString());
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.warn(
            "error while trying to deleteIndexBackup for index: {} for service: {}, resource: {}, nDays: {}",
            request.getIndexName(),
            request.getServiceName(),
            request.getResourceName(),
            request.getNDays(),
            e);
        responseObserver.onError(
            Status.UNKNOWN
                .withCause(e)
                .withDescription(
                    String.format(
                        "error while trying to deleteIndexBackup for index %s for service: %s, resource: %s, nDays: %s",
                        request.getIndexName(),
                        request.getServiceName(),
                        request.getResourceName(),
                        request.getNDays()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }

    @Override
    public void backupWarmingQueries(
        BackupWarmingQueriesRequest request,
        StreamObserver<BackupWarmingQueriesResponse> responseObserver) {
      String index = request.getIndex();
      try {
        IndexState indexState = globalState.getIndex(index);
        Warmer warmer = indexState.getWarmer();
        if (warmer == null) {
          logger.warn("Unable to backup warming queries as warmer not found for index: {}", index);
          responseObserver.onError(
              Status.UNKNOWN
                  .withDescription(
                      "Unable to backup warming queries as warmer not found for index: " + index)
                  .asRuntimeException());
          return;
        }
        int numQueriesThreshold = request.getNumQueriesThreshold();
        int numWarmingRequests = warmer.getNumWarmingRequests();
        if (numQueriesThreshold > 0 && numWarmingRequests < numQueriesThreshold) {
          logger.warn(
              "Unable to backup warming queries since warmer has {} requests, which is less than threshold {}",
              numWarmingRequests,
              numQueriesThreshold);
          responseObserver.onError(
              Status.UNKNOWN
                  .withDescription(
                      String.format(
                          "Unable to backup warming queries since warmer has %s requests, which is less than threshold %s",
                          numWarmingRequests, numQueriesThreshold))
                  .asRuntimeException());
          return;
        }
        int uptimeMinutesThreshold = request.getUptimeMinutesThreshold();
        int currUptimeMinutes =
            (int) (ManagementFactory.getRuntimeMXBean().getUptime() / 1000L / 60L);
        if (uptimeMinutesThreshold > 0 && currUptimeMinutes < uptimeMinutesThreshold) {
          logger.warn(
              "Unable to backup warming queries since uptime is {} minutes, which is less than threshold {}",
              currUptimeMinutes,
              uptimeMinutesThreshold);
          responseObserver.onError(
              Status.UNKNOWN
                  .withDescription(
                      String.format(
                          "Unable to backup warming queries since uptime is %s minutes, which is less than threshold %s",
                          currUptimeMinutes, uptimeMinutesThreshold))
                  .asRuntimeException());
          return;
        }
        warmer.backupWarmingQueriesToS3(request.getServiceName());
        responseObserver.onNext(BackupWarmingQueriesResponse.newBuilder().build());
        responseObserver.onCompleted();
      } catch (IOException e) {
        logger.error(
            "Unable to backup warming queries for index: {}, service: {}",
            index,
            request.getServiceName(),
            e);
        responseObserver.onError(
            Status.UNKNOWN
                .withCause(e)
                .withDescription(
                    String.format(
                        "Unable to backup warming queries for index: %s, service: %s",
                        index, request.getServiceName()))
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
        ShardState shardState = indexState.getShards().get(0);
        logger.info("Beginning force merge for index: {}", forceMergeRequest.getIndexName());
        shardState.writer.forceMerge(
            forceMergeRequest.getMaxNumSegments(), forceMergeRequest.getDoWait());
      } catch (IOException e) {
        logger.warn("Error during force merge for index {} ", forceMergeRequest.getIndexName(), e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "Error during force merge for index " + forceMergeRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
        return;
      }

      ForceMergeResponse.Status status =
          forceMergeRequest.getDoWait()
              ? ForceMergeResponse.Status.FORCE_MERGE_COMPLETED
              : ForceMergeResponse.Status.FORCE_MERGE_SUBMITTED;
      logger.info("Force merge status: {}", status);
      ForceMergeResponse response = ForceMergeResponse.newBuilder().setStatus(status).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }

    @Override
    public void forceMergeDeletes(
        ForceMergeDeletesRequest forceMergeRequest,
        StreamObserver<ForceMergeDeletesResponse> responseObserver) {
      if (forceMergeRequest.getIndexName().isEmpty()) {
        responseObserver.onError(new IllegalArgumentException("Index name in request is empty"));
        return;
      }

      try {
        IndexState indexState = globalState.getIndex(forceMergeRequest.getIndexName());
        ShardState shardState = indexState.getShards().get(0);
        logger.info(
            "Beginning force merge deletes for index: {}", forceMergeRequest.getIndexName());
        shardState.writer.forceMergeDeletes(forceMergeRequest.getDoWait());
      } catch (IOException e) {
        logger.warn(
            "Error during force merge deletes for index {} ", forceMergeRequest.getIndexName(), e);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    "Error during force merge deletes for index "
                        + forceMergeRequest.getIndexName())
                .augmentDescription(e.getMessage())
                .asRuntimeException());
        return;
      }

      ForceMergeDeletesResponse.Status status =
          forceMergeRequest.getDoWait()
              ? ForceMergeDeletesResponse.Status.FORCE_MERGE_DELETES_COMPLETED
              : ForceMergeDeletesResponse.Status.FORCE_MERGE_DELETES_SUBMITTED;
      logger.info("Force merge deletes status: {}", status);
      ForceMergeDeletesResponse response =
          ForceMergeDeletesResponse.newBuilder().setStatus(status).build();
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
            if (globalState.getConfiguration().getFileSendDelay()) {
              randomDelay(ThreadLocalRandom.current());
            }
          }
          // EOF
          rawFileChunkStreamObserver.onCompleted();
        }
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
              indexState = globalState.getIndex(fileInfoRequest.getIndexName());
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
