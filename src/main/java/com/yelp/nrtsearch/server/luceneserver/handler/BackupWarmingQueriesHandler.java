/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.handler;

import com.yelp.nrtsearch.server.grpc.BackupWarmingQueriesRequest;
import com.yelp.nrtsearch.server.grpc.BackupWarmingQueriesResponse;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.warming.Warmer;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupWarmingQueriesHandler
    extends Handler<BackupWarmingQueriesRequest, BackupWarmingQueriesResponse> {
  private static final Logger logger = LoggerFactory.getLogger(BackupWarmingQueriesHandler.class);

  public BackupWarmingQueriesHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(
      BackupWarmingQueriesRequest request,
      StreamObserver<BackupWarmingQueriesResponse> responseObserver) {
    logger.info("Received backup warming queries request: {}", request);
    String index = request.getIndex();
    try {
      IndexState indexState = getGlobalState().getIndex(index);
      Warmer warmer = indexState.getWarmer();
      if (warmer == null) {
        logger.warn("Unable to backup warming queries as warmer not found for index: {}", index);
        responseObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    "Unable to backup warming queries as warmer not found for index: " + index)
                .asRuntimeException());
        return;
      }
      int numQueriesThreshold = request.getNumQueriesThreshold();
      int numWarmingRequests = warmer.getNumWarmingRequests();
      if (numQueriesThreshold > 0 && numWarmingRequests < numQueriesThreshold) {
        logger.warn(
            "Unable to backup warming queries since warmer has {} requests, which is less than threshold {}",
            numWarmingRequests,
            numQueriesThreshold);
        responseObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    String.format(
                        "Unable to backup warming queries since warmer has %s requests, which is less than threshold %s",
                        numWarmingRequests, numQueriesThreshold))
                .asRuntimeException());
        return;
      }
      int uptimeMinutesThreshold = request.getUptimeMinutesThreshold();
      int currUptimeMinutes =
          (int) (ManagementFactory.getRuntimeMXBean().getUptime() / 1000L / 60L);
      if (uptimeMinutesThreshold > 0 && currUptimeMinutes < uptimeMinutesThreshold) {
        logger.warn(
            "Unable to backup warming queries since uptime is {} minutes, which is less than threshold {}",
            currUptimeMinutes,
            uptimeMinutesThreshold);
        responseObserver.onError(
            Status.UNKNOWN
                .withDescription(
                    String.format(
                        "Unable to backup warming queries since uptime is %s minutes, which is less than threshold %s",
                        currUptimeMinutes, uptimeMinutesThreshold))
                .asRuntimeException());
        return;
      }
      warmer.backupWarmingQueriesToS3(request.getServiceName());
      responseObserver.onNext(BackupWarmingQueriesResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (IOException e) {
      logger.error(
          "Unable to backup warming queries for index: {}, service: {}",
          index,
          request.getServiceName(),
          e);
      responseObserver.onError(
          Status.UNKNOWN
              .withCause(e)
              .withDescription(
                  String.format(
                      "Unable to backup warming queries for index: %s, service: %s",
                      index, request.getServiceName()))
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }
}
