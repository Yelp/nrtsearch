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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.config.ThreadPoolConfiguration;
import com.yelp.nrtsearch.server.config.YamlConfigReader;
import com.yelp.nrtsearch.server.utils.ThreadPoolExecutorFactory;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import java.io.ByteArrayInputStream;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GrpcServerExecutorSupplierTest {

  @BeforeClass
  public static void setUp() {
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(
            new YamlConfigReader(
                new ByteArrayInputStream(
                    "threadPoolConfiguration:\n  search:\n    maxThreads: 1".getBytes())));
    ThreadPoolExecutorFactory.init(threadPoolConfiguration);
  }

  @Test
  public void testGetExecutor() {

    GrpcServerExecutorSupplier grpcServerExecutorSupplier = new GrpcServerExecutorSupplier();

    ServerCall metricsServerCall = getMockServerCall("metrics");
    ServerCall searchServerCall = getMockServerCall("search");
    Metadata mockMetadata = mock(Metadata.class);

    assertEquals(
        grpcServerExecutorSupplier.getMetricsThreadPoolExecutor(),
        grpcServerExecutorSupplier.getExecutor(metricsServerCall, mockMetadata));
    assertEquals(
        grpcServerExecutorSupplier.getLuceneServerThreadPoolExecutor(),
        grpcServerExecutorSupplier.getExecutor(searchServerCall, mockMetadata));
  }

  private ServerCall getMockServerCall(String methodName) {
    ServerCall serverCall = mock(ServerCall.class);
    MethodDescriptor methodDescriptor = mock(MethodDescriptor.class);
    when(methodDescriptor.getBareMethodName()).thenReturn(methodName);
    when(serverCall.getMethodDescriptor()).thenReturn(methodDescriptor);
    return serverCall;
  }
}
