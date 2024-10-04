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
package com.yelp.nrtsearch.server.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.FetchTask;
import com.yelp.nrtsearch.server.plugins.FetchTaskPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class FetchTaskCreatorTest {

  @Before
  public void init() {
    init(Collections.emptyList());
  }

  private void init(List<Plugin> plugins) {
    FetchTaskCreator.initialize(getEmptyConfig(), plugins);
  }

  private LuceneServerConfiguration getEmptyConfig() {
    String config = "nodeName: \"lucene_server_foo\"";
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  public static class TestFetchTaskPlugin extends Plugin implements FetchTaskPlugin {
    @Override
    public Map<String, FetchTaskProvider<? extends FetchTasks.FetchTask>> getFetchTasks() {
      return Collections.singletonMap("custom_fetch_task", new TestFetchTaskProvider());
    }

    public static class TestFetchTaskProvider implements FetchTaskProvider<TestFetchTask> {
      @Override
      public TestFetchTask get(Map<String, Object> params) {
        return new TestFetchTask(params);
      }
    }

    public static class TestFetchTask implements FetchTasks.FetchTask {
      Map<String, Object> params;

      TestFetchTask(Map<String, Object> params) {
        this.params = params;
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCustomFetchTaskNotDefined() {
    FetchTask fetchTask = FetchTask.newBuilder().setName("custom_fetch_task").build();
    FetchTaskCreator.getInstance().createFetchTask(fetchTask);
  }

  @Test
  public void testPluginProvidesFetchTask() {
    init(Collections.singletonList(new TestFetchTaskPlugin()));
    FetchTask grpcFetchTask = FetchTask.newBuilder().setName("custom_fetch_task").build();
    FetchTasks.FetchTask fetchTask = FetchTaskCreator.getInstance().createFetchTask(grpcFetchTask);
    assertTrue(fetchTask instanceof TestFetchTaskPlugin.TestFetchTask);
  }

  @Test
  public void testNoParamsSet() {
    init(Collections.singletonList(new TestFetchTaskPlugin()));
    FetchTask grpcFetchTask = FetchTask.newBuilder().setName("custom_fetch_task").build();
    FetchTasks.FetchTask fetchTask = FetchTaskCreator.getInstance().createFetchTask(grpcFetchTask);
    assertTrue(fetchTask instanceof TestFetchTaskPlugin.TestFetchTask);

    TestFetchTaskPlugin.TestFetchTask testFetchTask = (TestFetchTaskPlugin.TestFetchTask) fetchTask;
    assertEquals(0, testFetchTask.params.size());
  }

  @Test
  public void testParams() {
    init(Collections.singletonList(new TestFetchTaskPlugin()));
    FetchTask grpcFetchTask =
        FetchTask.newBuilder()
            .setName("custom_fetch_task")
            .setParams(
                Struct.newBuilder()
                    .putFields("number_param", Value.newBuilder().setNumberValue(1.11).build())
                    .putFields(
                        "text_param", Value.newBuilder().setStringValue("test_param").build())
                    .build())
            .build();
    FetchTasks.FetchTask fetchTask = FetchTaskCreator.getInstance().createFetchTask(grpcFetchTask);
    assertTrue(fetchTask instanceof TestFetchTaskPlugin.TestFetchTask);

    TestFetchTaskPlugin.TestFetchTask testFetchTask = (TestFetchTaskPlugin.TestFetchTask) fetchTask;
    assertEquals(2, testFetchTask.params.size());
    assertEquals(testFetchTask.params.get("number_param"), 1.11);
    assertEquals(testFetchTask.params.get("text_param"), "test_param");
  }
}
