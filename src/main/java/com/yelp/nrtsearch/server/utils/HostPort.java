package com.yelp.nrtsearch.server.utils;

import java.util.Objects;

public class HostPort {
    private final String hostName;
    private final int port;
    public HostPort(String hostName, int port ) {
        this.hostName = hostName;
        this.port=port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HostPort hostPort = (HostPort) o;
        return port == hostPort.port &&
                Objects.equals(hostName, hostPort.hostName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostName, port);
    }

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HostPort{");
        sb.append("hostname='").append(hostName).append('\'');
        sb.append(", port=").append(port);
        sb.append('}');
        return sb.toString();
    }
}
