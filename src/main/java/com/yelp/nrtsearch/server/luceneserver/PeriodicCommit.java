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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeriodicCommit {

  Logger logger = LoggerFactory.getLogger(IndexState.class);

  private final IndexState indexState;
  private final TimerTask periodicCommitTask;
  private final Timer timer;

  public PeriodicCommit(IndexState indexState) {
    this.indexState = indexState;
    timer = new Timer();
    periodicCommitTask =
        new TimerTask() {
          @Override
          public void run() {
            try {
              indexState.commit();
              logger.error("running periodic commit for index: {}", indexState.name);
            } catch (IOException e) {
              logger.warn("error while trying to commit index: {}", indexState.name, e);
            }
          }
        };
  }

  public void schedule() {
    long delaySec = indexState.globalState.configuration.getPeriodicCommitDelaySec();

    if (delaySec != LuceneServerConfiguration.PERIODIC_COMMIT_OFF && delaySec > 0) {
      timer.scheduleAtFixedRate(periodicCommitTask, 0, delaySec * 1000);
    }
  }

  public void cancel() {
    timer.cancel();
  }
}
