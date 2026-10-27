/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.remote.s3;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import software.amazon.awssdk.transfer.s3.model.TransferObjectRequest;
import software.amazon.awssdk.transfer.s3.progress.TransferListener.Context;
import software.amazon.awssdk.transfer.s3.progress.TransferProgressSnapshot;

public class S3ProgressListenerImplTest {

  private S3ProgressListenerImpl createListener(AtomicLong deltaSum) {
    S3ProgressListenerImpl listener = new S3ProgressListenerImpl("service", "resource", "op", 1000);
    listener.setDeltaCallback(deltaSum::addAndGet);
    return listener;
  }

  private Context.BytesTransferred context(TransferObjectRequest request, long cumulativeBytes) {
    TransferProgressSnapshot snapshot = mock(TransferProgressSnapshot.class);
    when(snapshot.transferredBytes()).thenReturn(cumulativeBytes);
    Context.BytesTransferred context = mock(Context.BytesTransferred.class);
    when(context.progressSnapshot()).thenReturn(snapshot);
    when(context.request()).thenReturn(request);
    return context;
  }

  @Test
  public void testRecordBytes_invokesDeltaCallback() {
    AtomicLong deltaSum = new AtomicLong();
    S3ProgressListenerImpl listener = createListener(deltaSum);
    listener.recordBytes(10);
    listener.recordBytes(5);
    assertEquals(15, deltaSum.get());
  }

  @Test
  public void testRecordBytes_zeroDeltaDoesNotInvokeCallback() {
    AtomicLong callCount = new AtomicLong();
    S3ProgressListenerImpl listener = new S3ProgressListenerImpl("service", "resource", "op", -1);
    listener.setDeltaCallback(delta -> callCount.incrementAndGet());
    listener.recordBytes(0);
    assertEquals(0, callCount.get());
  }

  @Test
  public void testRecordBytes_withoutCallback() {
    S3ProgressListenerImpl listener = new S3ProgressListenerImpl("service", "resource", "op", 100);
    listener.recordBytes(10);
  }

  @Test
  public void testBytesTransferred_computesPerRequestDeltas() {
    AtomicLong deltaSum = new AtomicLong();
    S3ProgressListenerImpl listener = createListener(deltaSum);
    TransferObjectRequest requestA = mock(TransferObjectRequest.class);
    TransferObjectRequest requestB = mock(TransferObjectRequest.class);

    // cumulative counts per request: A 100 -> 250, B 40
    listener.bytesTransferred(context(requestA, 100));
    listener.bytesTransferred(context(requestA, 250));
    listener.bytesTransferred(context(requestB, 40));

    assertEquals(290, deltaSum.get());
  }

  @Test
  public void testBytesTransferredAndRecordBytes_shareTotals() {
    AtomicLong deltaSum = new AtomicLong();
    S3ProgressListenerImpl listener = createListener(deltaSum);
    TransferObjectRequest request = mock(TransferObjectRequest.class);

    listener.bytesTransferred(context(request, 100));
    listener.recordBytes(50);
    listener.bytesTransferred(context(request, 160));

    assertEquals(210, deltaSum.get());
  }
}
