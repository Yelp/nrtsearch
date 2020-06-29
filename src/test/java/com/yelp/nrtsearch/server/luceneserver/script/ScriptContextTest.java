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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class ScriptContextTest {
  public interface TestFactoryWithFactoryInterface {
    TestStatefulFactoryInterface newFactory(int a, long b);
  }

  public interface TestFactoryWithFactoryAbstract {
    TestStatefulFactoryAbstract newFactory(int a, long b);
  }

  public interface TestFactoryWithInstance {
    TestInstance newInstance(int a, long b, double c, float d);
  }

  public interface TestStatefulFactoryInterface {
    TestInstance newInstance(double c, float d);
  }

  public abstract static class TestStatefulFactoryAbstract {
    TestStatefulFactoryAbstract(int a, long b) {}

    public abstract TestInstance newInstance(double c, float d);
  }

  public abstract static class TestInstance {
    public static final String[] PARAMETERS = new String[] {};

    TestInstance(int a, long b, double c, float d) {}

    public abstract Object execute();
  }

  interface InvalidFactoryNone {}

  interface InvalidFactoryBoth {
    TestStatefulFactoryInterface newFactory(int a, long b);

    TestInstance newInstance(int a, long b, double c, float d);
  }

  interface InvalidFactoryMultiFactory {
    TestStatefulFactoryInterface newFactory(int a, long b);

    TestStatefulFactoryInterface newFactory(int a, long b, double c);
  }

  interface InvalidFactoryMultiInstance {
    TestInstance newInstance(int a, long b, double c, float d);

    TestInstance newInstance(int a, long b, double c, float d, boolean e);
  }

  @Test
  public void testContextWithStatefulInterface() {
    ScriptContext<TestFactoryWithFactoryInterface> context =
        new ScriptContext<>("test", TestFactoryWithFactoryInterface.class);
    assertEquals(TestFactoryWithFactoryInterface.class, context.factoryClazz);
    assertEquals(TestStatefulFactoryInterface.class, context.statefulFactoryClazz);
    assertEquals(TestInstance.class, context.instanceClazz);
  }

  @Test
  public void testContextWithStatefulAbstract() {
    ScriptContext<TestFactoryWithFactoryAbstract> context =
        new ScriptContext<>("test", TestFactoryWithFactoryAbstract.class);
    assertEquals(TestFactoryWithFactoryAbstract.class, context.factoryClazz);
    assertEquals(TestStatefulFactoryAbstract.class, context.statefulFactoryClazz);
    assertEquals(TestInstance.class, context.instanceClazz);
  }

  @Test
  public void testContextWithInstance() {
    ScriptContext<TestFactoryWithInstance> context =
        new ScriptContext<>("test", TestFactoryWithInstance.class);
    assertEquals(TestFactoryWithInstance.class, context.factoryClazz);
    assertNull(context.statefulFactoryClazz);
    assertEquals(TestInstance.class, context.instanceClazz);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNeitherMethodDefined() {
    new ScriptContext<InvalidFactoryNone>("test", InvalidFactoryNone.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBothMethodsDefined() {
    new ScriptContext<InvalidFactoryBoth>("test", InvalidFactoryBoth.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultiGetFactoryMethodsDefined() {
    new ScriptContext<InvalidFactoryMultiFactory>("test", InvalidFactoryMultiFactory.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultiGetInstanceMethodsDefined() {
    new ScriptContext<InvalidFactoryMultiInstance>("test", InvalidFactoryMultiInstance.class);
  }
}
