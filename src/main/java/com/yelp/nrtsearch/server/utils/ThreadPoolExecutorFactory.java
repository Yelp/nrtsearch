package com.yelp.nrtsearch.server.utils;

import com.yelp.nrtsearch.server.grpc.LuceneServer;
import org.apache.lucene.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolExecutorFactory {
    public enum ExecutorType {
        SEARCH,
        INDEX,
        LUCENESERVER,
        REPLICATIONSERVER
    }

    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolExecutorFactory.class.getName());
    private static final int MAX_SEARCHING_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int MAX_BUFFERED_ITEMS = Math.max(100, 2 * MAX_SEARCHING_THREADS);
    // Seems to be substantially faster than ArrayBlockingQueue at high throughput:
    private static final BlockingQueue<Runnable> docsToIndex = new LinkedBlockingQueue<Runnable>(MAX_BUFFERED_ITEMS);
    private final static int MAX_INDEXING_THREADS = MAX_SEARCHING_THREADS;
    private final static int MAX_GRPC_LUCENESERVER_THREADS = MAX_SEARCHING_THREADS;
    private final static int MAX_GRPC_REPLICATIONSERVER_THREADS = MAX_SEARCHING_THREADS;

    public static ThreadPoolExecutor getThreadPoolExecutor(ExecutorType executorType) {
        if (executorType.equals(ExecutorType.SEARCH)) {
            logger.info("Creating LuceneSearchExecutor of size " + MAX_SEARCHING_THREADS);
            //same as Executors.newFixedThreadPool except we want a NamedThreadFactory instead of defaultFactory
            return new ThreadPoolExecutor(MAX_SEARCHING_THREADS,
                    MAX_SEARCHING_THREADS,
                    0, TimeUnit.SECONDS,
                    docsToIndex,
                    new NamedThreadFactory("LuceneSearchExecutor"));

        } else if (executorType.equals(ExecutorType.INDEX)) {
            logger.info("Creating LuceneIndexingExecutor of size " + MAX_INDEXING_THREADS);
            return new ThreadPoolExecutor(MAX_INDEXING_THREADS,
                    MAX_INDEXING_THREADS,
                    0, TimeUnit.SECONDS,
                    docsToIndex,
                    new NamedThreadFactory("LuceneIndexingExecutor"));
        } else if (executorType.equals(ExecutorType.LUCENESERVER)) {
            logger.info("Creating GrpcLuceneServerExecutor of size " + MAX_GRPC_LUCENESERVER_THREADS);
            return new ThreadPoolExecutor(MAX_GRPC_LUCENESERVER_THREADS,
                    MAX_GRPC_LUCENESERVER_THREADS,
                    0, TimeUnit.SECONDS,
                    docsToIndex,
                    new NamedThreadFactory("GrpcLuceneServerExecutor"));
        } else if (executorType.equals(ExecutorType.REPLICATIONSERVER)) {
            logger.info("Creating GrpcReplicationServerExecutor of size " + MAX_GRPC_REPLICATIONSERVER_THREADS);
            return new ThreadPoolExecutor(MAX_GRPC_REPLICATIONSERVER_THREADS,
                    MAX_GRPC_REPLICATIONSERVER_THREADS,
                    0, TimeUnit.SECONDS,
                    docsToIndex,
                    new NamedThreadFactory("GrpcReplicationServerExecutor"));
        } else {
            throw new RuntimeException("Invalid executor type provided " + executorType.toString());
        }
    }
}
