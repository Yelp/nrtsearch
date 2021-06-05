/*
 * Copyright 2021 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.yelp.nrtsearch.server.luceneserver.warming;

import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.utils.Archiver;
import org.apache.lucene.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Warmer {
    private static final Logger logger = LoggerFactory.getLogger(Warmer.class);
    private static final String WARMING_QUERIES_RESOURCE = "warming_queries";
    private static final String WARMING_QUERIES_FILE = "warming_queries.txt";

    private final Archiver archiver;
    private final String serviceName;
    private final String resourceName;
    private final List<SearchRequest> warmingRequests;
    private final ReservoirSampler reservoirSampler;

    public Warmer(Archiver archiver, String serviceName, String indexName, int maxWarmingQueries) {
        this.archiver = archiver;
        this.serviceName = serviceName;
        this.resourceName = indexName + WARMING_QUERIES_RESOURCE;
        this.warmingRequests = Collections.synchronizedList(new ArrayList<>(maxWarmingQueries));
        this.reservoirSampler = new ReservoirSampler(maxWarmingQueries);
    }

    public void addSearchRequest(SearchRequest searchRequest) {
        ReservoirSampler.SampleResult sampleResult = reservoirSampler.sample(searchRequest.getIndexName());
        if (sampleResult.isSample()) {
            warmingRequests.set(sampleResult.getReplace(), searchRequest);
        }
    }

    public void backupWarmingQueriesToS3() throws IOException {
        Path warmingQueriesDir = null;
        Path warmingQueriesFile = null;
        BufferedWriter writer = null;
        try {
            warmingQueriesDir = Files.createTempDirectory("warming_queries");
            warmingQueriesFile = warmingQueriesDir.resolve(WARMING_QUERIES_FILE);
            writer = Files.newBufferedWriter(warmingQueriesFile);
            for (SearchRequest searchRequest : warmingRequests) {
                writer.write(JsonFormat.printer().print(searchRequest));
                writer.newLine();
            }
            writer.flush();
            writer.close();
            writer = null;
            archiver.upload(serviceName, resourceName, warmingQueriesDir, List.of(), List.of(), true);
        } finally {
            if (writer != null) {
                writer.close();
            }
            if (warmingQueriesFile != null && Files.exists(warmingQueriesFile)) {
                Files.delete(warmingQueriesFile);
            }
            if (warmingQueriesDir != null && Files.exists(warmingQueriesDir)) {
                Files.delete(warmingQueriesDir);
            }
        }
    }

    public void warmFromS3(IndexState indexState, int parallelism) throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
        if (archiver.getVersionedResource(serviceName, resourceName).isEmpty()) {
            logger.info("No warming queries found in S3 for service: {} and resource: {}", serviceName, resourceName);
            return;
        }
        ThreadPoolExecutor threadPoolExecutor = null;
        if (parallelism > 1) {
            int numThreads = parallelism - 1;
            threadPoolExecutor = new ThreadPoolExecutor(numThreads, numThreads, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(0), new NamedThreadFactory("warming-"), new ThreadPoolExecutor.CallerRunsPolicy());
        }
        Path downloadDir = null;
        try {
            downloadDir = archiver.download(serviceName, resourceName);
            BufferedReader reader = Files.newBufferedReader(downloadDir.resolve(WARMING_QUERIES_FILE));
            String line;
            while ((line = reader.readLine()) != null) {
                SearchRequest.Builder builder = SearchRequest.newBuilder();
                JsonFormat.parser().merge(line, builder);
                SearchRequest searchRequest = builder.build();
                SearchHandler searchHandler = new SearchHandler(indexState.getSearchThreadPoolExecutor());
                if (threadPoolExecutor == null) {
                    searchHandler.handle(indexState, searchRequest);
                } else {
                    threadPoolExecutor.submit(() -> searchHandler.handle(indexState, searchRequest));
                }
            }
        } finally {
            if (threadPoolExecutor != null) {
                threadPoolExecutor.shutdown();
                threadPoolExecutor.awaitTermination(10, TimeUnit.SECONDS);
            }
            if (downloadDir != null && Files.exists(downloadDir)) {
                for (Path file : Files.list(downloadDir).collect(Collectors.toList())) {
                    Files.delete(file);
                }
                Files.delete(downloadDir);
            }
        }

    }

}
