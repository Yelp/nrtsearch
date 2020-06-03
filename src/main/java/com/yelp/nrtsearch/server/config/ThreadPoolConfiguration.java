package com.yelp.nrtsearch.server.config;

/**
 * Configuration for various ThreadPool Settings used in nrtsearch
 */
public class ThreadPoolConfiguration {

    private static final int DEFAULT_MAX_SEARCHING_THREADS = ((Runtime.getRuntime().availableProcessors() * 3) / 2) + 1;
    private static final int DEFAULT_MAX_SEARCH_BUFFERED_ITEMS = Math.max(1000, 2 * DEFAULT_MAX_SEARCHING_THREADS);

    private static final int DEFAULT_MAX_INDEXING_THREADS = Runtime.getRuntime().availableProcessors() + 1;
    private static final int DEFAULT_MAX_INDEXING_BUFFERED_ITEMS = Math.max(200, 2 * DEFAULT_MAX_INDEXING_THREADS);

    private final static int DEFAULT_MAX_GRPC_LUCENESERVER_THREADS = DEFAULT_MAX_INDEXING_THREADS;
    private static final int DEFAULT_MAX_GRPC_LUCENESERVER_BUFFERED_ITEMS = DEFAULT_MAX_INDEXING_BUFFERED_ITEMS;

    private final static int DEFAULT_MAX_GRPC_REPLICATIONSERVER_THREADS = DEFAULT_MAX_INDEXING_THREADS;
    private static final int DEFAULT_MAX_GRPC_REPLICATIONSERVER_BUFFERED_ITEMS = DEFAULT_MAX_INDEXING_BUFFERED_ITEMS;

    private int maxSearchingThreads = DEFAULT_MAX_SEARCHING_THREADS;
    private int maxSearchBufferedItems = DEFAULT_MAX_SEARCH_BUFFERED_ITEMS;

    private int maxIndexingThreads = DEFAULT_MAX_INDEXING_THREADS;
    private int maxIndexingBufferedItems = DEFAULT_MAX_INDEXING_BUFFERED_ITEMS;

    private int maxGrpcLuceneserverThreads = DEFAULT_MAX_GRPC_LUCENESERVER_THREADS;
    private int maxGrpcLuceneserverBufferedItems = DEFAULT_MAX_GRPC_LUCENESERVER_BUFFERED_ITEMS;

    private int maxGrpcReplicationserverThreads = DEFAULT_MAX_GRPC_REPLICATIONSERVER_THREADS;
    private int maxGrpcReplicationserverBufferedItems = DEFAULT_MAX_GRPC_REPLICATIONSERVER_BUFFERED_ITEMS;

    public int getMaxSearchingThreads() {
        return maxSearchingThreads;
    }

    public void setMaxSearchingThreads(int maxSearchingThreads) {
        this.maxSearchingThreads = maxSearchingThreads;
    }

    public int getMaxSearchBufferedItems() {
        return maxSearchBufferedItems;
    }

    public void setMaxSearchBufferedItems(int maxSearchBufferedItems) {
        this.maxSearchBufferedItems = maxSearchBufferedItems;
    }

    public int getMaxIndexingThreads() {
        return maxIndexingThreads;
    }

    public void setMaxIndexingThreads(int maxIndexingThreads) {
        this.maxIndexingThreads = maxIndexingThreads;
    }

    public int getMaxIndexingBufferedItems() {
        return maxIndexingBufferedItems;
    }

    public void setMaxIndexingBufferedItems(int maxIndexingBufferedItems) {
        this.maxIndexingBufferedItems = maxIndexingBufferedItems;
    }

    public int getMaxGrpcLuceneserverThreads() {
        return maxGrpcLuceneserverThreads;
    }

    public void setMaxGrpcLuceneserverThreads(int maxGrpcLuceneserverThreads) {
        this.maxGrpcLuceneserverThreads = maxGrpcLuceneserverThreads;
    }

    public int getMaxGrpcReplicationserverThreads() {
        return maxGrpcReplicationserverThreads;
    }

    public void setMaxGrpcReplicationserverThreads(int maxGrpcReplicationserverThreads) {
        this.maxGrpcReplicationserverThreads = maxGrpcReplicationserverThreads;
    }

    public int getMaxGrpcLuceneserverBufferedItems() {
        return maxGrpcLuceneserverBufferedItems;
    }

    public void setMaxGrpcLuceneserverBufferedItems(int maxGrpcLuceneserverBufferedItems) {
        this.maxGrpcLuceneserverBufferedItems = maxGrpcLuceneserverBufferedItems;
    }

    public int getMaxGrpcReplicationserverBufferedItems() {
        return maxGrpcReplicationserverBufferedItems;
    }

    public void setMaxGrpcReplicationserverBufferedItems(int maxGrpcReplicationserverBufferedItems) {
        this.maxGrpcReplicationserverBufferedItems = maxGrpcReplicationserverBufferedItems;
    }
}
