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

package org.apache.platypus.server.luceneserver;

import com.google.gson.*;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class GlobalState implements Closeable, Restorable {
    // TODO: make these controllable
    // nocommit allow controlling per CSV/json bulk import max concurrency sent into IW?
    private final static int MAX_INDEXING_THREADS = Runtime.getRuntime().availableProcessors();
    public static final String NULL = "NULL";
    private final String hostName;
    private final int port;
    private final int replicationPort;

    Logger logger = LoggerFactory.getLogger(GlobalState.class);
    Gson gson = new Gson();
    private long lastIndicesGen;
    private final JsonParser jsonParser = new JsonParser();

    public final String nodeName;

    public final List<RemoteNodeConnection> remoteNodes = new CopyOnWriteArrayList<>();

    private final static int MAX_BUFFERED_ITEMS = Math.max(100, 2 * MAX_INDEXING_THREADS);

    // Seems to be substantially faster than ArrayBlockingQueue at high throughput:
    final BlockingQueue<Runnable> docsToIndex = new LinkedBlockingQueue<Runnable>(MAX_BUFFERED_ITEMS);

    //same as Executors.newFixedThreadPool except we want a NamedThreadFactory instead of defaultFactory
    private final ExecutorService indexService = new ThreadPoolExecutor(MAX_INDEXING_THREADS,
            MAX_INDEXING_THREADS,
            0, TimeUnit.SECONDS,
            docsToIndex,
            new NamedThreadFactory("LuceneIndexing"));


    /**
     * Current indices.
     */
    final Map<String, IndexState> indices = new ConcurrentHashMap<String, IndexState>();

    /**
     * Server shuts down once this latch is decremented.
     */
    public final CountDownLatch shutdownNow = new CountDownLatch(1);

    final Path stateDir;

    /**
     * This is persisted so on restart we know about all previously created indices.
     */
    private final JsonObject indexNames = new JsonObject();

    /**
     * Sole constructor.
     */
    public GlobalState(String nodeName, Path stateDir, String hostName, int port, int replicationPort) throws IOException {
        logger.info("MAX INDEXING THREADS " + MAX_INDEXING_THREADS);
        this.nodeName = nodeName;
        this.stateDir = stateDir;
        this.hostName = hostName;
        this.port = port;
        this.replicationPort = replicationPort;
        if (Files.exists(stateDir) == false) {
            Files.createDirectories(stateDir);
        }
        //TODO: figure if we need SearchQueue when we get searching
        //searchQueue = new SearchQueue(this);
        loadIndexNames();
    }

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    public Path getStateDir() {
        return stateDir;
    }

    public synchronized void setStateDir(Path source) throws IOException {
        restoreDir(source, stateDir);
        loadIndexNames();
    }

    //need to call this first time LuceneServer comes up and upon StartIndex with restore
    private void loadIndexNames() throws IOException {
        long gen = IndexState.getLastGen(stateDir, "indices");
        lastIndicesGen = gen;
        if (gen != -1) {
            Path path = stateDir.resolve("indices." + gen);
            byte[] bytes;
            try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ)) {
                bytes = new byte[(int) channel.size()];
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                int count = channel.read(buffer);
                if (count != bytes.length) {
                    throw new AssertionError("fix me!");
                }
            }
            JsonObject o;
            try {
                o = jsonParser.parse(IndexState.fromUTF8(bytes)).getAsJsonObject();
            } catch (JsonParseException pe) {
                // Something corrupted the save state since we last
                // saved it ...
                throw new RuntimeException("index state file \"" + path + "\" cannot be parsed: " + pe.getMessage());
            }
            for (Map.Entry<String, JsonElement> ent : o.entrySet()) {
                indexNames.add(ent.getKey(), ent.getValue());
            }
        }
    }

    private void saveIndexNames() throws IOException {
        synchronized (indices) {
            lastIndicesGen++;
            byte[] bytes = IndexState.toUTF8(indexNames.toString());
            Path f = stateDir.resolve("indices." + lastIndicesGen);
            try (FileChannel channel = FileChannel.open(f, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
                int count = channel.write(ByteBuffer.wrap(bytes));
                if (count != bytes.length) {
                    throw new AssertionError("fix me");
                }
                channel.force(true);
            }

            // remove old gens
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateDir)) {
                for (Path sub : stream) {
                    if (sub.toString().startsWith("indices.")) {
                        long gen = Long.parseLong(sub.toString().substring(8));
                        if (gen != lastIndicesGen) {
                            Files.delete(sub);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        logger.info("GlobalState.close: indices=" + indices);
        //searchThread.interrupt();
        IOUtils.close(remoteNodes);
        IOUtils.close(indices.values());
        indexService.shutdown();
        TimeLimitingCollector.getGlobalTimerThread().stopTimer();
        try {
            TimeLimitingCollector.getGlobalTimerThread().join();
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

    /**
     * Create a new index.
     */
    public IndexState createIndex(String name, Path rootDir) throws IllegalArgumentException, IOException {
        synchronized (indices) {
            if (indexNames.get(name) != null) {
                throw new IllegalArgumentException("index \"" + name + "\" already exists");
            }
            if (rootDir == null) {
                indexNames.addProperty(name, NULL);
            } else {
                if (Files.exists(rootDir)) {
                    throw new IllegalArgumentException("rootDir \"" + rootDir + "\" already exists");
                }
                indexNames.addProperty(name, rootDir.toAbsolutePath().toString());
            }
            saveIndexNames();
            IndexState state = new IndexState(this, name, rootDir, true, false);
            indices.put(name, state);
            return state;
        }
    }

    public IndexState getIndex(String name, boolean hasRestore) throws IOException {
        synchronized (indices) {
            IndexState state = indices.get(name);
            if (state == null) {
                String rootPath = indexNames.get(name).getAsString();
                if (rootPath != null) {
                    if (rootPath.equals(NULL)) {
                        state = new IndexState(this, name, null, false, hasRestore);
                    } else {
                        state = new IndexState(this, name, Paths.get(rootPath), false, hasRestore);
                    }
                    // nocommit we need to also persist which shards are here?
                    state.addShard(0, false);
                    indices.put(name, state);
                } else {
                    throw new IllegalArgumentException("index \"" + name + "\" was not yet created");
                }
            }
            return state;
        }

    }

    /**
     * Get the {@link IndexState} by index name.
     */
    public IndexState getIndex(String name) throws IllegalArgumentException, IOException {
        return getIndex(name, false);
    }

    public Future<Long> submitIndexingTask(Callable job) throws InterruptedException {
        return indexService.submit(job);
    }

    /**
     * Remove the specified index.
     */
    public void deleteIndex(String name) {
        synchronized (indices) {
            indexNames.remove(name);
        }
    }

    public int getReplicationPort() {
        return replicationPort;
    }

}
