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
package com.yelp.nrtsearch.server.luceneserver.state;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.Restorable;
import com.yelp.nrtsearch.server.luceneserver.index.LegacyIndexState;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GlobalState implementation that uses legacy state management. Index names are written to an
 * indices.{#} file, which is restored on startup.
 */
public class LegacyGlobalState extends GlobalState implements Restorable {
  public static final String NULL = "NULL";
  private static final Logger logger = LoggerFactory.getLogger(LegacyGlobalState.class);
  private long lastIndicesGen;
  private final JsonParser jsonParser = new JsonParser();

  /** Current indices. */
  private final Map<String, IndexState> indices = new ConcurrentHashMap<>();

  /** This is persisted so on restart we know about all previously created indices. */
  private final JsonObject indexNames = new JsonObject();

  public LegacyGlobalState(
      LuceneServerConfiguration luceneServerConfiguration, Archiver incArchiver)
      throws IOException {
    super(luceneServerConfiguration, incArchiver);
    loadIndexNames();
  }

  // need to call this first time LuceneServer comes up and upon StartIndex with restore
  private void loadIndexNames() throws IOException {
    long gen = LegacyIndexState.getLastGen(getStateDir(), "indices");
    lastIndicesGen = gen;
    if (gen != -1) {
      Path path = getStateDir().resolve("indices." + gen);
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
        throw new RuntimeException(
            "index state file \"" + path + "\" cannot be parsed: " + pe.getMessage());
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
      Path f = getStateDir().resolve("indices." + lastIndicesGen);
      try (FileChannel channel =
          FileChannel.open(f, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
        int count = channel.write(ByteBuffer.wrap(bytes));
        if (count != bytes.length) {
          throw new AssertionError("fix me");
        }
        channel.force(true);
      }

      // remove old gens
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(getStateDir())) {
        for (Path sub : stream) {
          String filename = sub.getFileName().toString();
          if (filename.startsWith("indices.")) {
            long gen = Long.parseLong(filename.substring(8));
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
    IOUtils.close(indices.values());
    super.close();
  }

  @Override
  public synchronized void setStateDir(Path source) throws IOException {
    restoreDir(source, getStateDir());
    loadIndexNames();
  }

  /** Create a new index. */
  @Override
  public IndexState createIndex(String name) throws IOException {
    synchronized (indices) {
      Path rootDir = getIndexDir(name);
      if (indexNames.get(name) != null) {
        throw new IllegalArgumentException("index \"" + name + "\" already exists");
      }
      if (rootDir == null) {
        indexNames.addProperty(name, NULL);
      } else {
        if (Files.exists(rootDir)) {
          throw new IllegalArgumentException("rootDir \"" + rootDir + "\" already exists");
        }
        indexNames.addProperty(name, name);
      }
      saveIndexNames();
      IndexState state = IndexState.createState(this, name, rootDir, true, false);
      indices.put(name, state);
      return state;
    }
  }

  @Override
  public IndexState getIndex(String name, boolean hasRestore) throws IOException {
    synchronized (indices) {
      IndexState state = indices.get(name);
      if (state == null) {
        String rootPath = null;
        JsonElement indexJsonName = indexNames.get(name);
        if (indexJsonName == null) {
          throw new IllegalArgumentException("index " + name + " was not saved or commited");
        }
        String indexName = indexJsonName.getAsString();
        if (indexName != null) {
          rootPath = getIndexDir(name).toString();
        }
        if (rootPath != null) {
          if (rootPath.equals(NULL)) {
            state = IndexState.createState(this, name, null, false, hasRestore);
          } else {
            state = IndexState.createState(this, name, Paths.get(rootPath), false, hasRestore);
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

  /** Get the {@link IndexState} by index name. */
  @Override
  public IndexState getIndex(String name) throws IOException {
    return getIndex(name, false);
  }

  @Override
  public void indexClosed(String name) {
    synchronized (indices) {
      indices.remove(name);
    }
  }

  /** Remove the specified index. */
  @Override
  public void deleteIndex(String name) throws IOException {
    synchronized (indices) {
      indexNames.remove(name);
      saveIndexNames();
    }
  }

  @Override
  public Set<String> getIndexNames() {
    return Collections.unmodifiableSet(indexNames.keySet());
  }
}
