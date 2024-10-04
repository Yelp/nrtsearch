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
package com.yelp.nrtsearch.server.luceneserver.nrt;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.junit.Test;

public class ProportionalCopyThreadTest {

  private CopyJob getJob(boolean highPriority) throws IOException {
    ReplicaNode mockNode = mock(ReplicaNode.class);
    when(mockNode.getFilesToCopy(null)).thenReturn(Collections.emptyList());
    return new SimpleCopyJob("", null, null, mockNode, null, highPriority, null, "", "", true);
  }

  @Test
  public void testHasJob_empty() {
    ProportionalCopyThread pct = new ProportionalCopyThread(null, 10);
    assertFalse(pct.hasJob());
  }

  @Test
  public void testHasJob_highPriority() throws IOException {
    ProportionalCopyThread pct = new ProportionalCopyThread(null, 10);
    CopyJob job = getJob(true);
    pct.addJob(job);
    assertTrue(pct.hasJob());
  }

  @Test
  public void testHadJob_lowPriority() throws IOException {
    ProportionalCopyThread pct = new ProportionalCopyThread(null, 10);
    CopyJob job = getJob(false);
    pct.addJob(job);
    assertTrue(pct.hasJob());
  }

  @Test
  public void testHadJob_mixedPriority() throws IOException {
    ProportionalCopyThread pct = new ProportionalCopyThread(null, 10);
    CopyJob jobHigh = getJob(true);
    CopyJob jobLow = getJob(false);
    pct.addJob(jobHigh);
    pct.addJob(jobLow);
    assertTrue(pct.hasJob());
  }

  @Test
  public void testOnlyHighPriority() throws IOException {
    ProportionalCopyThread pct = new ProportionalCopyThread(null, 10);
    CopyJob job = getJob(true);
    pct.addJob(job);

    for (int i = 0; i < 1000; i++) {
      CopyJob nextJob = pct.getJob();
      assertSame(nextJob, job);
      pct.addJob(nextJob);
    }
  }

  @Test
  public void testOnlyLowPriority() throws IOException {
    ProportionalCopyThread pct = new ProportionalCopyThread(null, 10);
    CopyJob job = getJob(false);
    pct.addJob(job);

    for (int i = 0; i < 1000; i++) {
      CopyJob nextJob = pct.getJob();
      assertSame(nextJob, job);
      pct.addJob(nextJob);
    }
  }

  @Test
  public void testMixedPriority() throws IOException {
    ProportionalCopyThread pct = new ProportionalCopyThread(null, 10);
    CopyJob jobHigh = getJob(true);
    CopyJob jobLow = getJob(false);
    pct.addJob(jobHigh);
    pct.addJob(jobLow);

    int highCount = 0;
    int lowCount = 0;
    for (int i = 0; i < 1000; i++) {
      CopyJob nextJob = pct.getJob();
      if (nextJob == jobHigh) {
        highCount++;
      } else if (nextJob == jobLow) {
        lowCount++;
      } else {
        fail();
      }
      pct.addJob(nextJob);
    }
    assertEquals(100, lowCount);
    assertEquals(900, highCount);
  }

  @Test
  public void testGetJob_empty() {
    ProportionalCopyThread pct = new ProportionalCopyThread(null, 10);
    assertNull(pct.getJob());
  }

  @Test
  public void testLowPercentage() {
    try {
      new ProportionalCopyThread(null, -1);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "lowPriorityCopyPercentage must be between 0 and 100, inclusive. Got: -1",
          e.getMessage());
    }
  }

  @Test
  public void testHighPercentage() {
    try {
      new ProportionalCopyThread(null, 101);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "lowPriorityCopyPercentage must be between 0 and 100, inclusive. Got: 101",
          e.getMessage());
    }
  }
}
