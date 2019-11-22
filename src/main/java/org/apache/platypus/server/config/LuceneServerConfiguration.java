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

package org.apache.platypus.server.config;

import java.nio.file.Path;
import java.nio.file.Paths;

public class LuceneServerConfiguration {
    public final static Path DEFAULT_USER_STATE_DIR = Paths.get(System.getProperty("user.home"), "lucene", "server");

    private int port;
    private int replicationPort;
    private String nodeName;
    private String stateDir;
    private String hostName;

    public int getPort() {
        return port;
    }

    public int getReplicationPort() {
        return replicationPort;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getStateDir() {
        return stateDir;
    }

    public String getHostName() {
        return hostName;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setReplicationPort(int replicationPort) {
        this.replicationPort = replicationPort;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void setStateDir(String stateDir) {
        this.stateDir = stateDir;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LuceneServerConfiguration{");
        sb.append("port=").append(port);
        sb.append(", replicationPort=").append(replicationPort);
        sb.append(", nodeName='").append(nodeName).append('\'');
        sb.append(", stateDir=").append(stateDir);
        sb.append(", hostName='").append(hostName).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public static class Builder {
        private String hostName;
        private int replicationPort;
        private int port;
        private String nodeName;
        private Path stateDir;

        public Builder() {
            //set default values
            this.nodeName = "main";
            this.hostName = "localhost";
            this.stateDir = DEFAULT_USER_STATE_DIR;
            this.port = 50051;
            this.replicationPort = 50052;
        }

        public Builder withStateDir(String tempStateDir) {
            this.stateDir = Paths.get(stateDir.toString(), tempStateDir);
            return this;
        }

        public Builder withNodeName(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }


        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withReplicationPort(int replicationPort) {
            this.replicationPort = replicationPort;
            return this;
        }

        public Builder withHostName(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public LuceneServerConfiguration build() {
            LuceneServerConfiguration luceneServerConfiguration = new LuceneServerConfiguration();
            luceneServerConfiguration.nodeName = this.nodeName;
            luceneServerConfiguration.stateDir = this.stateDir.toString();
            luceneServerConfiguration.port = this.port;
            luceneServerConfiguration.hostName = this.hostName;
            luceneServerConfiguration.replicationPort = this.replicationPort;
            return luceneServerConfiguration;
        }

    }


}
