/*
 *
 *  *
 *  *  Copyright 2019 Yelp Inc.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  *  either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *
 *
 *
 */

package org.apache.platypus.server;

import org.apache.platypus.server.config.LuceneServerConfiguration;
import org.apache.platypus.server.grpc.Mode;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.platypus.server.config.LuceneServerConfiguration.DEFAULT_USER_DIR;

public class LuceneServerTestConfigurationFactory {
    static AtomicLong atomicLong = new AtomicLong();

    public static LuceneServerConfiguration getConfig(Mode mode) {
        String dirNum = String.valueOf(atomicLong.addAndGet(1));
        LuceneServerConfiguration.Builder builder = new LuceneServerConfiguration.Builder();
        if (mode.equals(Mode.STANDALONE)) {
            String stateDir = Paths.get(DEFAULT_USER_DIR.toString(), "standalone", dirNum, "state").toString();
            String indexDir = Paths.get(DEFAULT_USER_DIR.toString(), "standalone", dirNum, "index").toString();
            return builder.withNodeName("standalone")
                    .withStateDir(stateDir)
                    .withIndexDir(indexDir)
                    .withPort(9000)
                    .withReplicationPort(9000)
                    .build();
        } else if (mode.equals(Mode.PRIMARY)) {
            String stateDir = Paths.get(DEFAULT_USER_DIR.toString(), "primary", dirNum, "state").toString();
            String indexDir = Paths.get(DEFAULT_USER_DIR.toString(), "primary", dirNum, "index").toString();
            return builder.withNodeName("primary")
                    .withStateDir(stateDir)
                    .withIndexDir(indexDir)
                    .withPort(9900)
                    .withReplicationPort(9001)
                    .build();
        } else if (mode.equals(Mode.REPLICA)) {
            String stateDir = Paths.get(DEFAULT_USER_DIR.toString(), "replica", dirNum, "state").toString();
            String indexDir = Paths.get(DEFAULT_USER_DIR.toString(), "replica", dirNum, "index").toString();
            return builder.withNodeName("replica")
                    .withStateDir(stateDir)
                    .withIndexDir(indexDir)
                    .withPort(9902)
                    .withReplicationPort(9003)
                    .build();
        }
        throw new RuntimeException("Invalid mode %s, cannot build config" + mode);
    }
}
