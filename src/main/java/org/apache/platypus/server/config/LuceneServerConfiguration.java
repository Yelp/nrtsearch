package org.apache.platypus.server.config;

import java.nio.file.Path;
import java.nio.file.Paths;

public class LuceneServerConfiguration {

    private int port;
    private int replicationPort;
    private String nodeName;
    private Path stateDir;
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

    public Path getStateDir() {
        return stateDir;
    }

    public String getHostName() {
        return hostName;
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
            this.stateDir = Paths.get(System.getProperty("user.home"), "lucene", "server", String.valueOf(ProcessHandle.current().pid()));
            this.port = 50051;
            this.replicationPort = 50052;
        }

        public Builder withStateDir(Path stateDir) {
            this.stateDir = stateDir;
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
            luceneServerConfiguration.stateDir = this.stateDir;
            luceneServerConfiguration.port = this.port;
            luceneServerConfiguration.hostName = this.hostName;
            luceneServerConfiguration.replicationPort = this.replicationPort;
            return luceneServerConfiguration;
        }

    }

    //private Constructor
    private LuceneServerConfiguration() {
        //noop
    }


}
