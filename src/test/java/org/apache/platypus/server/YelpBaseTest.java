package org.apache.platypus.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.platypus.server.grpc.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

public class YelpBaseTest {
    protected static final Logger logger = Logger.getLogger(YelpReviewsTest.class.getName());

    protected static void liveSettings(LuceneServerClient serverClient, String indexName) {
        LiveSettingsRequest liveSettingsRequest = LiveSettingsRequest.newBuilder()
                .setIndexName(indexName)
                .setIndexRamBufferSizeMB(256.0)
                .setMaxRefreshSec(1.0)
                .build();
        LiveSettingsResponse liveSettingsResponse = serverClient.getBlockingStub().liveSettings(liveSettingsRequest);
        logger.info(liveSettingsResponse.getResponse());
    }

    protected static String readResourceAsString(String path) throws IOException {
        return Files.readString(Paths.get(path));
    }


    protected static void registerFields(LuceneServerClient serverClient, String path) throws IOException {
        String registerFieldsJson = readResourceAsString(path);
        FieldDefRequest fieldDefRequest = getFieldDefRequest(registerFieldsJson);
        FieldDefResponse fieldDefResponse = serverClient.getBlockingStub().registerFields(fieldDefRequest);
        logger.info(fieldDefResponse.getResponse());

    }


    private static FieldDefRequest getFieldDefRequest(String jsonStr) {
        logger.fine(String.format("Converting fields %s to proto FieldDefRequest", jsonStr));
        FieldDefRequest.Builder fieldDefRequestBuilder = FieldDefRequest.newBuilder();
        try {
            JsonFormat.parser().merge(jsonStr, fieldDefRequestBuilder);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        FieldDefRequest fieldDefRequest = fieldDefRequestBuilder.build();
        logger.fine(String.format("jsonStr converted to proto FieldDefRequest %s", fieldDefRequest.toString()));
        return fieldDefRequest;
    }

    protected static void createIndex(LuceneServerClient serverClient, Path dir, String indexName) {
        CreateIndexResponse response = serverClient.getBlockingStub().createIndex(
                CreateIndexRequest.newBuilder()
                        .setIndexName(indexName)
                        .setRootDir(dir.resolve("index").toString())
                        .build());
        logger.info(response.getResponse());
    }

    protected static void startIndex(LuceneServerClient serverClient, StartIndexRequest startIndexRequest) {
        StartIndexResponse startIndexResponse = serverClient
                .getBlockingStub().startIndex(startIndexRequest);
        logger.info(
                String.format("numDocs: %s, maxDoc: %s, segments: %s, startTimeMS: %s",
                        startIndexResponse.getNumDocs(),
                        startIndexResponse.getMaxDoc(),
                        startIndexResponse.getSegments(),
                        startIndexResponse.getStartTimeMS()));
    }
}
