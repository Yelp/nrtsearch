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
package com.yelp.nrtsearch.server.nrt;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.nrt.jobs.SimpleCopyJob;
import java.io.IOException;
import java.util.Collections;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.junit.Test;

public class DefaultCopyThreadTest {
  private CopyJob getJob(boolean highPriority) throws IOException {
    ReplicaNode mockNode = mock(ReplicaNode.class);
    when(mockNode.getFilesToCopy(null)).thenReturn(Collections.emptyList());
    return new SimpleCopyJob("", null, null, mockNode, null, highPriority, null, "", "", true);
  }

  @Test
  public void testHasJob_empty() {
    DefaultCopyThread pct = new DefaultCopyThread(null);
    assertFalse(pct.hasJob());
  }

  @Test
  public void testHasJob() throws IOException {
    DefaultCopyThread pct = new DefaultCopyThread(null);
    CopyJob job = getJob(true);
    pct.addJob(job);
    assertTrue(pct.hasJob());
  }

  @Test
  public void testOnlyHighPriority() throws IOException {
    DefaultCopyThread pct = new DefaultCopyThread(null);
    CopyJob job = getJob(true);
    pct.addJob(job);

    for (int i = 0; i < 100; i++) {
      CopyJob nextJob = pct.getJob();
      assertSame(nextJob, job);
      pct.addJob(nextJob);
    }
  }

  @Test
  public void testOnlyLowPriority() throws IOException {
    DefaultCopyThread pct = new DefaultCopyThread(null);
    CopyJob job = getJob(false);
    pct.addJob(job);

    for (int i = 0; i < 100; i++) {
      CopyJob nextJob = pct.getJob();
      assertSame(nextJob, job);
      pct.addJob(nextJob);
    }
  }

  @Test
  public void testMixedPriority() throws IOException {
    DefaultCopyThread pct = new DefaultCopyThread(null);
    CopyJob jobHigh = getJob(true);
    CopyJob jobLow = getJob(false);
    pct.addJob(jobHigh);
    pct.addJob(jobLow);

    for (int i = 0; i < 100; i++) {
      CopyJob nextJob = pct.getJob();
      assertSame(nextJob, jobHigh);
      pct.addJob(nextJob);
    }
  }

  @Test
  public void testGetJob_empty() {
    DefaultCopyThread pct = new DefaultCopyThread(null);
    assertNull(pct.getJob());
  }
}
