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
package com.yelp.nrtsearch.server.luceneserver.script;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.ScriptPlugin;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class ScriptServiceTest {

  @Before
  public void init() {
    init(Collections.emptyList());
  }

  private void init(List<Plugin> plugins) {
    ScriptService.initialize(getEmptyConfig(), plugins);
  }

  private LuceneServerConfiguration getEmptyConfig() {
    String config = "nodeName: \"lucene_server_foo\"";
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  static class TestScriptPlugin extends Plugin implements ScriptPlugin {
    @Override
    public Iterable<ScriptEngine> getScriptEngines(List<ScriptContext<?>> contexts) {
      return Arrays.asList(new TestScriptEngine(), new TestNullFactoryEngine());
    }
  }

  static class TestScriptEngine implements ScriptEngine {

    @Override
    public String getLang() {
      return "my_lang";
    }

    @Override
    public <T> T compile(String source, ScriptContext<T> context) {
      ScoreScript.Factory factory = ((params, docLookup) -> null);
      return context.factoryClazz.cast(factory);
    }
  }

  static class TestNullFactoryEngine implements ScriptEngine {

    @Override
    public String getLang() {
      return "null_lang";
    }

    @Override
    public <T> T compile(String source, ScriptContext<T> context) {
      return null;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testScriptEngineNotPresent() {
    Script script = Script.newBuilder().setLang("my_lang").setSource("my_source").build();
    ScriptService.getInstance().compile(script, ScoreScript.CONTEXT);
  }

  @Test
  public void testPluginAddedEngine() {
    init(Collections.singletonList(new TestScriptPlugin()));
    Script script = Script.newBuilder().setLang("my_lang").setSource("my_source").build();
    ScoreScript.Factory factory = ScriptService.getInstance().compile(script, ScoreScript.CONTEXT);
    assertNotNull(factory);
  }

  @Test
  public void testCompileCached() {
    Script script1 = Script.newBuilder().setLang("js").setSource("3.0*4.0").build();
    Script script2 = Script.newBuilder().setLang("js").setSource("5.0*4.0").build();
    ScoreScript.Factory factory1 =
        ScriptService.getInstance().compile(script1, ScoreScript.CONTEXT);
    ScoreScript.Factory factory2 =
        ScriptService.getInstance().compile(script2, ScoreScript.CONTEXT);
    assertNotSame(factory1, factory2);
    assertSame(factory2, ScriptService.getInstance().compile(script2, ScoreScript.CONTEXT));
    assertSame(factory1, ScriptService.getInstance().compile(script1, ScoreScript.CONTEXT));
  }

  @Test
  public void testCompileCachedWithDifferentParams() {
    Map<String, Script.ParamValue> params1 = new HashMap<>();
    params1.put("param_1", Script.ParamValue.newBuilder().setLongValue(10).build());
    params1.put("param_2", Script.ParamValue.newBuilder().setFloatValue(2.22F).build());
    params1.put("param_3", Script.ParamValue.newBuilder().setDoubleValue(1.11).build());

    Map<String, Script.ParamValue> params2 = new HashMap<>();
    params2.put("param_1", Script.ParamValue.newBuilder().setLongValue(7).build());
    params2.put("param_2", Script.ParamValue.newBuilder().setFloatValue(3.33F).build());
    params2.put("param_3", Script.ParamValue.newBuilder().setIntValue(25).build());

    Script script1 =
        Script.newBuilder()
            .setLang("js")
            .setSource("param_1*long_field+param_2*count+param_3*float_field")
            .putAllParams(params1)
            .build();
    Script script2 =
        Script.newBuilder()
            .setLang("js")
            .setSource("param_1*long_field+param_2*count+param_3*float_field")
            .putAllParams(params2)
            .build();
    Script script3 =
        Script.newBuilder()
            .setLang("js")
            .setSource("param_1*long_field+param_2*count+param_3*float_field")
            .build();
    Script script4 =
        Script.newBuilder().setLang("js").setSource("param_1*long_field+param_2*count").build();

    ScoreScript.Factory factory1 =
        ScriptService.getInstance().compile(script1, ScoreScript.CONTEXT);
    ScoreScript.Factory factory2 =
        ScriptService.getInstance().compile(script2, ScoreScript.CONTEXT);
    ScoreScript.Factory factory3 =
        ScriptService.getInstance().compile(script3, ScoreScript.CONTEXT);
    ScoreScript.Factory factory4 =
        ScriptService.getInstance().compile(script4, ScoreScript.CONTEXT);
    assertSame(factory1, factory2);
    assertSame(factory2, factory3);
    assertNotSame(factory1, factory4);
  }

  @Test(expected = UncheckedExecutionException.class)
  public void testNullFactory() {
    init(Collections.singletonList(new TestScriptPlugin()));
    Script script = Script.newBuilder().setLang("null_lang").setSource("my_source").build();
    ScriptService.getInstance().compile(script, ScoreScript.CONTEXT);
  }
}
