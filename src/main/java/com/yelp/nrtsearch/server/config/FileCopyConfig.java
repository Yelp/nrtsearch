/*
 * Copyright 2021 Yelp Inc.
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
package com.yelp.nrtsearch.server.config;

/** Configuration class for file copying files to replicas. */
public class FileCopyConfig {
  static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
  static final int DEFAULT_ACK_EVERY = 1000;
  static final int DEFAULT_MAX_IN_FLIGHT = 2000;

  private final boolean ackedCopy;
  private final int chunkSize;
  private final int ackEvery;
  private final int maxInFlight;

  /**
   * Create instance from provided configuration reader.
   *
   * @param configReader config reader
   * @return class instance
   */
  public static FileCopyConfig fromConfig(YamlConfigReader configReader) {
    boolean ackedCopy = configReader.getBoolean("FileCopyConfig.ackedCopy", false);
    int chunkSize = configReader.getInteger("FileCopyConfig.chunkSize", DEFAULT_CHUNK_SIZE);
    int ackEvery = configReader.getInteger("FileCopyConfig.ackEvery", DEFAULT_ACK_EVERY);
    int maxInFlight = configReader.getInteger("FileCopyConfig.maxInFlight", DEFAULT_MAX_IN_FLIGHT);
    return new FileCopyConfig(ackedCopy, chunkSize, ackEvery, maxInFlight);
  }

  /**
   * Constructor.
   *
   * @param ackedCopy if acked file copy should be used
   * @param chunkSize file chunk size
   * @param ackEvery chunks to send between acks
   * @param maxInFlight maximum in flight chunks
   */
  public FileCopyConfig(boolean ackedCopy, int chunkSize, int ackEvery, int maxInFlight) {
    if (ackEvery > maxInFlight) {
      throw new IllegalArgumentException("ackEvery must be less than or equal to maxInFlight");
    }
    this.ackedCopy = ackedCopy;
    this.chunkSize = chunkSize;
    this.ackEvery = ackEvery;
    this.maxInFlight = maxInFlight;
  }

  /** Get if acked copy should be used. */
  public boolean getAckedCopy() {
    return ackedCopy;
  }

  /** Get copy chunk size. */
  public int getChunkSize() {
    return chunkSize;
  }

  /** Get chunks to send between acks. */
  public int getAckEvery() {
    return ackEvery;
  }

  /** Get maximum chunks in flight. */
  public int getMaxInFlight() {
    return maxInFlight;
  }
}
