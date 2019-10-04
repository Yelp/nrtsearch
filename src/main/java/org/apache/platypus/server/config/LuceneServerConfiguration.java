package org.apache.platypus.server.config;

import java.nio.file.Path;
import java.nio.file.Paths;

public class LuceneServerConfiguration {

    private int port;
    private String nodeName;
    private Path stateDir;

    public int getPort() {
        return port;
    }

    public String getNodeName() {
        return nodeName;
    }

    public Path getStateDir() {
        return stateDir;
    }


    public static class Builder {
        private int port;
        private String nodeName;
        private Path stateDir;

        public Builder() {
            //set default values
            this.nodeName = "main";
            this.stateDir = Paths.get(System.getProperty("user.home"), "lucene", "server");
            this.port = 50051;
        }

        public Builder withPort(Path port) {
            this.stateDir = port;
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

        public LuceneServerConfiguration build() {
            LuceneServerConfiguration luceneServerConfiguration = new LuceneServerConfiguration();
            luceneServerConfiguration.nodeName = this.nodeName;
            luceneServerConfiguration.stateDir = this.stateDir;
            luceneServerConfiguration.port = this.port;
            return luceneServerConfiguration;
        }

    }

    //private Constructor
    private LuceneServerConfiguration() {
        //noop
    }


}
