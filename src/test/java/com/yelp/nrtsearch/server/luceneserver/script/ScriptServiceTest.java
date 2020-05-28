/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.yelp.nrtsearch.server.luceneserver.script;

import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.ScriptPlugin;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class ScriptServiceTest {

    @Before
    public void init() {
        init(Collections.emptyList());
    }

    private void init(List<Plugin> plugins) {
        ScriptService.initialize(plugins);
    }

    static class TestScriptPlugin extends Plugin implements ScriptPlugin {
        public Iterable<ScriptEngine> getScriptEngines() {
            return Collections.singletonList(new TestScriptEngine());
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

    @Test(expected = IllegalArgumentException.class)
    public void testScriptEngineNotPresent() {
        Script script = Script.newBuilder()
                .setLang("my_lang")
                .setSource("my_source")
                .build();
        ScriptService.getInstance().compile(script, ScoreScript.CONTEXT);
    }

    @Test
    public void testPluginAddedEngine() {
        init(Collections.singletonList(new TestScriptPlugin()));
        Script script = Script.newBuilder()
                .setLang("my_lang")
                .setSource("my_source")
                .build();
        ScoreScript.Factory factory = ScriptService.getInstance().compile(script, ScoreScript.CONTEXT);
        assertNotNull(factory);
    }

    @Test
    public void testCompileCached() {
        Script script1 = Script.newBuilder()
                .setLang("js")
                .setSource("3.0*4.0")
                .build();
        Script script2 = Script.newBuilder()
                .setLang("js")
                .setSource("5.0*4.0")
                .build();
        ScoreScript.Factory factory1 = ScriptService.getInstance().compile(script1, ScoreScript.CONTEXT);
        ScoreScript.Factory factory2 = ScriptService.getInstance().compile(script2, ScoreScript.CONTEXT);
        assertNotSame(factory1, factory2);
        assertSame(factory2, ScriptService.getInstance().compile(script2, ScoreScript.CONTEXT));
        assertSame(factory1, ScriptService.getInstance().compile(script1, ScoreScript.CONTEXT));
    }
}
