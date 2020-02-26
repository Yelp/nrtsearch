/*
 *
 *  * Copyright 2019 Yelp Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  * either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.platypus.server.grpc;

import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.platypus.server.cli.AddDocumentsCommand;
import org.apache.platypus.server.cli.BackupIndexCommand;
import org.apache.platypus.server.cli.Cmd;
import org.apache.platypus.server.cli.CommitCommand;
import org.apache.platypus.server.cli.CreateIndexCommand;
import org.apache.platypus.server.cli.DeleteAllDocumentsCommand;
import org.apache.platypus.server.cli.DeleteDocumentsCommand;
import org.apache.platypus.server.cli.DeleteIndexCommand;
import org.apache.platypus.server.cli.GetCurrentSearcherVersion;
import org.apache.platypus.server.cli.LiveSettingsCommand;
import org.apache.platypus.server.cli.RefreshCommand;
import org.apache.platypus.server.cli.RegisterFieldsCommand;
import org.apache.platypus.server.cli.SearchCommand;
import org.apache.platypus.server.cli.SettingsCommand;
import org.apache.platypus.server.cli.StartIndexCommand;
import org.apache.platypus.server.cli.StatsCommand;
import org.apache.platypus.server.cli.StopIndexCommand;
import org.apache.platypus.server.cli.WriteNRTPointCommand;
import picocli.CommandLine;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static org.apache.platypus.server.cli.AddDocumentsCommand.ADD_DOCUMENTS;
import static org.apache.platypus.server.cli.BackupIndexCommand.BACKUP_INDEX;
import static org.apache.platypus.server.cli.CommitCommand.COMMIT;
import static org.apache.platypus.server.cli.CreateIndexCommand.CREATE_INDEX;
import static org.apache.platypus.server.cli.DeleteAllDocumentsCommand.DELETE_ALL_DOCS;
import static org.apache.platypus.server.cli.DeleteDocumentsCommand.DELETE_DOCS;
import static org.apache.platypus.server.cli.DeleteIndexCommand.DELETE_INDEX;
import static org.apache.platypus.server.cli.GetCurrentSearcherVersion.CURRENT_SEARCHER_VERSION;
import static org.apache.platypus.server.cli.LiveSettingsCommand.LIVE_SETTINGS;
import static org.apache.platypus.server.cli.RefreshCommand.REFRESH;
import static org.apache.platypus.server.cli.RegisterFieldsCommand.REGISTER_FIELDS;
import static org.apache.platypus.server.cli.SearchCommand.SEARCH;
import static org.apache.platypus.server.cli.SettingsCommand.SETTINGS;
import static org.apache.platypus.server.cli.StartIndexCommand.START_INDEX;
import static org.apache.platypus.server.cli.StatsCommand.STATS;
import static org.apache.platypus.server.cli.StatusCommand.STATUS;
import static org.apache.platypus.server.cli.StopIndexCommand.STOP_INDEX;
import static org.apache.platypus.server.cli.WriteNRTPointCommand.WRITE_NRT_POINT;

/**
 * A simple client that requests a greeting from the {@link LuceneServer}.
 */
public class LuceneServerClient {
    private static final Logger logger = Logger.getLogger(LuceneServerClient.class.getName());

    private final ManagedChannel channel;

    public LuceneServerGrpc.LuceneServerBlockingStub getBlockingStub() {
        return blockingStub;
    }

    public LuceneServerGrpc.LuceneServerStub getAsyncStub() {
        return asyncStub;
    }

    private final LuceneServerGrpc.LuceneServerBlockingStub blockingStub;
    private final LuceneServerGrpc.LuceneServerStub asyncStub;

    /**
     * Construct client connecting to LuceneServer server at {@code host:port}.
     */
    public LuceneServerClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build());
    }

    /**
     * Construct client for accessing LuceneServer server using the existing channel.
     */
    LuceneServerClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = LuceneServerGrpc.newBlockingStub(channel);
        asyncStub = LuceneServerGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void createIndex(String indexName, String rootDir) {
        logger.info("Will try to create index: " + indexName + " ,at rootDir: " + rootDir);
        CreateIndexRequest request = CreateIndexRequest.newBuilder().setIndexName(indexName).setRootDir(rootDir).build();
        CreateIndexResponse response;
        try {
            response = blockingStub.createIndex(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Server returned : " + response.getResponse());
    }

    public void liveSettings(String indexName, double maxRefreshSec, double minRefreshSec, double maxSearcherAgeSec, double indexRamBufferSizeMB) {
        logger.info(String.format("will try to update liveSettings for indexName: %s, " +
                        "maxRefreshSec: %s, minRefreshSec: %s, maxSearcherAgeSec: %s, indexRamBufferSizeMB: %s ", indexName,
                maxRefreshSec, minRefreshSec, maxSearcherAgeSec, indexRamBufferSizeMB));
        LiveSettingsRequest request = LiveSettingsRequest.newBuilder()
                .setIndexName(indexName)
                .setMaxRefreshSec(maxRefreshSec)
                .setMinRefreshSec(minRefreshSec)
                .setMaxSearcherAgeSec(maxSearcherAgeSec)
                .setIndexRamBufferSizeMB(indexRamBufferSizeMB).build();
        LiveSettingsResponse response;
        try {
            response = blockingStub.liveSettings(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Server returned : " + response.getResponse());
    }

    public void registerFields(String jsonStr) {
        FieldDefRequest fieldDefRequest = getFieldDefRequest(jsonStr);
        FieldDefResponse response;
        try {
            response = blockingStub.registerFields(fieldDefRequest);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Server returned : " + response.getResponse());
    }

    public void settings(Path filePath) throws IOException {
        SettingsRequest settingsRequest = new LuceneServerClientBuilder.SettingsClientBuilder().buildRequest(filePath);
        SettingsResponse response;
        try {
            response = blockingStub.settings(settingsRequest);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Server returned : " + response.getResponse());
    }

    public void startIndex(Path filePath) throws IOException {
        StartIndexRequest startIndexRequest = new LuceneServerClientBuilder.StartIndexClientBuilder().buildRequest(filePath);
        StartIndexResponse response;
        try {
            response = blockingStub.startIndex(startIndexRequest);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Server returned : " + response.toString());
    }

    public void addDocuments(Stream<AddDocumentRequest> addDocumentRequestStream) throws InterruptedException {
        final CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<AddDocumentResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(AddDocumentResponse value) {
                // Note that Server sends back only 1 message (Unary mode i.e. Server calls its onNext only once
                // which is when it is done with indexing the entire stream), which means this method should be
                // called only once.
                logger.info(String.format("Received response for genId: %s", value));
            }

            @Override
            public void onError(Throwable t) {
                logger.log(Level.SEVERE, t.getMessage(), t);
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.info(String.format("Received final response from server"));
                finishLatch.countDown();
            }
        };

        //The responseObserver handles responses from the server (i.e. 1 onNext and 1 completed)
        //The requestObserver handles the sending of stream of client requests to server (i.e. multiple onNext and 1 completed)
        StreamObserver<AddDocumentRequest> requestObserver = asyncStub.addDocuments(responseObserver);
        try {
            addDocumentRequestStream.forEach(addDocumentRequest -> requestObserver.onNext(addDocumentRequest));
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        // Mark the end of requests
        requestObserver.onCompleted();

        logger.info("sent async addDocumentsRequest to server...");

        // Receiving happens asynchronously, so block here for 5 minutes
        if (!finishLatch.await(5, TimeUnit.MINUTES)) {
            logger.log(Level.WARNING, "addDocuments can not finish within 5 minutes");
        }
    }

    public void refresh(String indexName) {
        logger.info("Will try to refresh index: " + indexName);
        RefreshRequest request = RefreshRequest.newBuilder().setIndexName(indexName).build();
        RefreshResponse response;
        try {
            response = blockingStub.refresh(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Server returned refreshTimeMS : " + response.getRefreshTimeMS());
    }

    public void commit(String indexName) {
        logger.info("Will try to commit index: " + indexName);
        CommitRequest request = CommitRequest.newBuilder().setIndexName(indexName).build();
        CommitResponse response;
        try {
            response = blockingStub.commit(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Server returned sequence id: " + response.getGen());
    }

    public void stats(String indexName) {
        logger.info("Will try to retrieve stats for index: " + indexName);
        StatsRequest request = StatsRequest.newBuilder().setIndexName(indexName).build();
        StatsResponse response;
        try {
            response = blockingStub.stats(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Server returned sequence id: " + response);
    }

    public void search(Path filePath) throws IOException {
        SearchRequest searchRequest = new LuceneServerClientBuilder.SearchClientBuilder().buildRequest(filePath);
        SearchResponse response;
        try {
            response = blockingStub.search(searchRequest);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Server returned : " + response.toString());
    }

    public void delete(Path filePath) throws IOException {
        AddDocumentRequest addDocumentRequest = new LuceneServerClientBuilder.DeleteDocumentsBuilder().buildRequest(filePath);
        AddDocumentResponse response;
        try {
            response = blockingStub.delete(addDocumentRequest);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Server returned indexGen : " + response.getGenId());
    }

    public void deleteIndex(String indexName) {
        DeleteIndexResponse response = blockingStub.deleteIndex(DeleteIndexRequest.newBuilder().setIndexName(indexName).build());
        logger.info("Server returned response : " + response.getOk());
    }

    public void deleteAllDocuments(String indexName) {
        DeleteAllDocumentsResponse response = blockingStub.deleteAll(DeleteAllDocumentsRequest.newBuilder().setIndexName(indexName).build());
        logger.info("Server returned genId : " + response.getGenId());
    }

    public void stopIndex(String indexName) {
        blockingStub.stopIndex(StopIndexRequest.newBuilder().setIndexName(indexName).build());
    }

    public void backupIndex(String indexName, String serviceName, String resourceName) {
        blockingStub.backupIndex(BackupIndexRequest.newBuilder()
                .setServiceName(serviceName)
                .setResourceName(resourceName)
                .setIndexName(indexName).build());
    }

    private void status() throws InterruptedException {
        try {
            HealthCheckResponse status = blockingStub.status(HealthCheckRequest.newBuilder()
                    .setCheck(true)
                    .build());
            if (status.getHealth() == TransferStatusCode.Done) {
                logger.info("Host is up");
                return;
            }
        } catch (StatusRuntimeException e) {
            logger.info(e.getMessage());
        }
        this.shutdown();
        System.exit(1);
    }


    private FieldDefRequest getFieldDefRequest(String jsonStr) {
        logger.info(String.format("Converting fields %s to proto FieldDefRequest", jsonStr));
        FieldDefRequest.Builder fieldDefRequestBuilder = FieldDefRequest.newBuilder();
        try {
            JsonFormat.parser().merge(jsonStr, fieldDefRequestBuilder);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        FieldDefRequest fieldDefRequest = fieldDefRequestBuilder.build();
        logger.info(String.format("jsonStr converted to proto FieldDefRequest %s", fieldDefRequest.toString()));
        return fieldDefRequest;
    }

    private SettingsRequest getSettingsRequest(String jsonStr) {
        logger.info(String.format("Converting fields %s to proto SettingsRequest", jsonStr));
        SettingsRequest.Builder settingsRequestBuilder = SettingsRequest.newBuilder();
        try {
            JsonFormat.parser().merge(jsonStr, settingsRequestBuilder);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        //set defaults
        if (settingsRequestBuilder.getNrtCachingDirectoryMaxMergeSizeMB() == 0) {
            settingsRequestBuilder.setNrtCachingDirectoryMaxMergeSizeMB(5.0);
        }
        if (settingsRequestBuilder.getNrtCachingDirectoryMaxSizeMB() == 0) {
            settingsRequestBuilder.setNrtCachingDirectoryMaxSizeMB(60.0);
        }
        if (settingsRequestBuilder.getDirectory().isEmpty()) {
            settingsRequestBuilder.setDirectory("FSDirectory");
        }
        if (settingsRequestBuilder.getNormsFormat().isEmpty()) {
            settingsRequestBuilder.setNormsFormat("Lucene80");
        }
        SettingsRequest settingsRequest = settingsRequestBuilder.build();
        logger.info(String.format("jsonStr converted to proto SettingsRequest %s", settingsRequest.toString()));
        return settingsRequest;
    }

    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting.
     */
    public static void main(String[] args) throws Exception {
        /* TODO: read host port from cmd. Access a service running on the local machine on port 50051 */
        CommandLine commandLine = new CommandLine(new Cmd());
        CommandLine.ParseResult cmdResult = commandLine.parseArgs(args);
        String subCommandStr = cmdResult.subcommand().commandSpec().name();
        Object subCommand = cmdResult.subcommand().commandSpec().userObject();
        Cmd baseComand = (Cmd) cmdResult.commandSpec().userObject();
        LuceneServerClient client = new LuceneServerClient(baseComand.getHostname(), baseComand.getPort());
        try {
            String jsonStr = "";
            Path filePath;
            switch (subCommandStr) {
                case CREATE_INDEX:
                    CreateIndexCommand createIndexCommand = (CreateIndexCommand) subCommand;
                    client.createIndex(createIndexCommand.getIndexName(), createIndexCommand.getRootDir());
                    break;
                case LIVE_SETTINGS:
                    LiveSettingsCommand liveSettingsCommand = (LiveSettingsCommand) subCommand;
                    client.liveSettings(liveSettingsCommand.getIndexName(), liveSettingsCommand.getMaxRefreshSec(),
                            liveSettingsCommand.getMinRefreshSec(), liveSettingsCommand.getMaxSearcherAgeSec(),
                            liveSettingsCommand.getIndexRamBufferSizeMB());
                    break;
                case REGISTER_FIELDS:
                    RegisterFieldsCommand registerFieldsCommand = (RegisterFieldsCommand) subCommand;
                    jsonStr = Files.readString(Paths.get(registerFieldsCommand.getFileName()));
                    client.registerFields(jsonStr);
                    break;
                case SETTINGS:
                    SettingsCommand settingsCommand = (SettingsCommand) subCommand;
                    filePath = Paths.get(settingsCommand.getFileName());
                    client.settings(filePath);
                    break;
                case START_INDEX:
                    StartIndexCommand startIndexCommand = (StartIndexCommand) subCommand;
                    filePath = Paths.get(startIndexCommand.getFileName());
                    client.startIndex(filePath);
                    break;
                case ADD_DOCUMENTS:
                    AddDocumentsCommand addDocumentsCommand = (AddDocumentsCommand) subCommand;
                    String indexName = addDocumentsCommand.getIndexName();
                    String fileType = addDocumentsCommand.getFileType();
                    Stream<AddDocumentRequest> addDocumentRequestStream;
                    filePath = Paths.get(addDocumentsCommand.getFileName());
                    if (fileType.equalsIgnoreCase("csv")) {
                        Reader reader = Files.newBufferedReader(filePath);
                        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
                        addDocumentRequestStream = new LuceneServerClientBuilder.AddDcoumentsClientBuilder(indexName, csvParser).buildRequest(filePath);
                        client.addDocuments(addDocumentRequestStream);
                    } else if (fileType.equalsIgnoreCase("json")) {
                        LuceneServerClientBuilder.AddJsonDocumentsClientBuilder addJsonDocumentsClientBuilder = new LuceneServerClientBuilder.AddJsonDocumentsClientBuilder(
                                indexName, new Gson(), filePath, addDocumentsCommand.getMaxBufferLen());
                        while (!addJsonDocumentsClientBuilder.isFinished()) {
                            addDocumentRequestStream = addJsonDocumentsClientBuilder.buildRequest(filePath);
                            client.addDocuments(addDocumentRequestStream);
                        }
                    } else {
                        throw new RuntimeException(String.format("%s  is not a  valid fileType", fileType));
                    }
                    break;
                case REFRESH:
                    RefreshCommand refreshCommand = (RefreshCommand) subCommand;
                    client.refresh(refreshCommand.getIndexName());
                    break;
                case COMMIT:
                    CommitCommand commitCommand = (CommitCommand) subCommand;
                    client.commit(commitCommand.getIndexName());
                    break;
                case STATS:
                    StatsCommand statsCommand = (StatsCommand) subCommand;
                    client.stats(statsCommand.getIndexName());
                    break;
                case SEARCH:
                    SearchCommand searchCommand = (SearchCommand) subCommand;
                    filePath = Paths.get(searchCommand.getFileName());
                    client.search(filePath);
                    break;
                case DELETE_DOCS:
                    DeleteDocumentsCommand deleteDocumentsCommand = (DeleteDocumentsCommand) subCommand;
                    filePath = Paths.get(deleteDocumentsCommand.getFileName());
                    client.delete(filePath);
                    break;
                case DELETE_ALL_DOCS:
                    DeleteAllDocumentsCommand deleteAllDocumentsCommand = (DeleteAllDocumentsCommand) subCommand;
                    client.deleteAllDocuments(deleteAllDocumentsCommand.getIndexName());
                    break;
                case DELETE_INDEX:
                    DeleteIndexCommand deleteIndexCommand = (DeleteIndexCommand) subCommand;
                    client.deleteIndex(deleteIndexCommand.getIndexName());
                    break;
                case STOP_INDEX:
                    StopIndexCommand stopIndexCommand = (StopIndexCommand) subCommand;
                    client.stopIndex(stopIndexCommand.getIndexName());
                    break;
                case WRITE_NRT_POINT:
                    WriteNRTPointCommand writeNRTPointCommand = (WriteNRTPointCommand) subCommand;
                    ReplicationServerClient replicationServerClient = new ReplicationServerClient(writeNRTPointCommand.getHostName(), writeNRTPointCommand.getPort());
                    SearcherVersion searcherVersion = replicationServerClient.writeNRTPoint(writeNRTPointCommand.getIndexName());
                    logger.info("didRefresh: " + searcherVersion.getDidRefresh());
                    logger.info("searcherVersion: " + searcherVersion.getVersion());
                    break;
                case CURRENT_SEARCHER_VERSION:
                    GetCurrentSearcherVersion getCurrentSearcherVersion = (GetCurrentSearcherVersion) subCommand;
                    ReplicationServerClient replServerClient = new ReplicationServerClient(getCurrentSearcherVersion.getHostName(), getCurrentSearcherVersion.getPort());
                    searcherVersion = replServerClient.getCurrentSearcherVersion(getCurrentSearcherVersion.getIndexName());
                    logger.info("searcherVersion: " + searcherVersion.getVersion());
                    break;
                case BACKUP_INDEX:
                    BackupIndexCommand backupIndexCommand = (BackupIndexCommand) subCommand;
                    client.backupIndex(backupIndexCommand.getIndexName(), backupIndexCommand.getServiceName(), backupIndexCommand.getResourceName());
                    break;
                case STATUS:
                    client.status();
                    break;
                default:
                    logger.warning(String.format("%s is not a valid server command", subCommandStr));
            }
        } finally {
            client.shutdown();
        }
    }
}

