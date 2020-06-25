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
package com.yelp.nrtsearch.server.utils;

import java.util.Objects;

public class HostPort {
  private final String hostName;
  private final int port;

  public HostPort(String hostName, int port) {
    this.hostName = hostName;
    this.port = port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HostPort hostPort = (HostPort) o;
    return port == hostPort.port && Objects.equals(hostName, hostPort.hostName);
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
