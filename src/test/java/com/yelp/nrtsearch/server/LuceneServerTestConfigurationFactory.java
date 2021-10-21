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
package com.yelp.nrtsearch.server;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.Mode;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

public class LuceneServerTestConfigurationFactory {
  static AtomicLong atomicLong = new AtomicLong();

  public static LuceneServerConfiguration getConfig(Mode mode, File dataRootDir) {
    return getConfig(mode, dataRootDir, Paths.get(dataRootDir.toString(), "archiver"), "");
  }

  public static LuceneServerConfiguration getConfig(
      Mode mode, File dataRootDir, String extraConfig) {
    return getConfig(mode, dataRootDir, Paths.get(dataRootDir.toString(), "archiver"), extraConfig);
  }

  public static LuceneServerConfiguration getConfig(
      Mode mode, File dataRootDir, Path archiverDirectory) {
    return getConfig(mode, dataRootDir, archiverDirectory, "");
  }

  public static LuceneServerConfiguration getConfig(
      Mode mode, File dataRootDir, Path archiverDirectory, String extraConfig) {
    String dirNum = String.valueOf(atomicLong.addAndGet(1));
    if (mode.equals(Mode.STANDALONE)) {
      String stateDir =
          Paths.get(dataRootDir.getAbsolutePath(), "standalone", dirNum, "state").toString();
      String indexDir =
          Paths.get(dataRootDir.getAbsolutePath(), "standalone", dirNum, "index").toString();
      String config =
          String.join(
              "\n",
              "nodeName: standalone",
              "stateDir: " + stateDir,
              "indexDir: " + indexDir,
              "port: " + (9700 + atomicLong.intValue()),
              "replicationPort: " + (17000 + atomicLong.intValue()),
              "archiveDirectory: " + archiverDirectory.toString(),
              extraConfig);
      return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
    } else if (mode.equals(Mode.PRIMARY)) {
      String stateDir =
          Paths.get(dataRootDir.getAbsolutePath(), "primary", dirNum, "state").toString();
      String indexDir =
          Paths.get(dataRootDir.getAbsolutePath(), "primary", dirNum, "index").toString();
      String config =
          String.join(
              "\n",
              "nodeName: primary",
              "stateDir: " + stateDir,
              "indexDir: " + indexDir,
              "port: " + 9900,
              "replicationPort: " + 9001,
              "archiveDirectory: " + archiverDirectory.toString(),
              extraConfig);
      return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
    } else if (mode.equals(Mode.REPLICA)) {
      String stateDir =
          Paths.get(dataRootDir.getAbsolutePath(), "replica", dirNum, "state").toString();
      String indexDir =
          Paths.get(dataRootDir.getAbsolutePath(), "replica", dirNum, "index").toString();
      String config =
          String.join(
              "\n",
              "nodeName: replica",
              "stateDir: " + stateDir,
              "indexDir: " + indexDir,
              "port: " + 9902,
              "replicationPort: " + 9003,
              extraConfig);
      return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
    }
    throw new RuntimeException("Invalid mode %s, cannot build config" + mode);
  }
}
