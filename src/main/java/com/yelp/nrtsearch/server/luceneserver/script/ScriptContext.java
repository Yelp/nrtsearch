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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Context that holds type information needed to compile a script. Provides classes conforming with
 * the script compile contract:
 *
 * <p>The factory Class must be an interface containing one of two methods:
 *
 * <pre>
 *     public interface Factory {
 *         Script newInstance(a, b);
 *     }
 *
 *     public interface Factory {
 *         StatefulFactory newFactory(a, b);
 *     }
 * </pre>
 *
 * The stateful factory must be an interface, or Class with an abstract newInstance method. If using
 * an abstract Class, it must contain a constructor with the same parameters as newFactory.
 *
 * <pre>
 *     public interface StatefulFactory {
 *         Script newInstance(c, d);
 *     }
 *
 *     public abstract class StatefulFactory {
 *         public StatefulFactory(a, b) {}
 *         public abstract Script newInstance(c, d);
 *     }
 * </pre>
 *
 * The script instance Class must contain an abstract execute method. The method may contain
 * parameters. There must be a static String[] called PARAMETERS defined in the class which lists
 * the label for each parameter (for potentially binding within a script).
 *
 * <p>There must be a constructor with the same parameters as newInstance (if not using stateful
 * factory), or with the concatenation of parameters from newFactory and newInstance (if using
 * stateful factory).
 *
 * <pre>
 *     public abstract class Script {
 *         public static final String[] PARAMETERS = new String[]{"e"};
 *         public Script(a, b, c, d) {}
 *         public abstract T execute(e);
 *     }
 * </pre>
 *
 * Though a stateful factory Class (if required) and instance Class must be provided, it is not
 * required that they be used by the {@link ScriptEngine}. For example, the {@link
 * com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine} compiles to a Factory that
 * produces a {@link org.apache.lucene.search.DoubleValuesSource} directly. This stateful factory
 * Class will not conform with the stateful factory contract. Instead, the abstract {@link
 * com.yelp.nrtsearch.server.luceneserver.script.ScoreScript.SegmentFactory} is provided as a {@link
 * org.apache.lucene.search.DoubleValuesSource} which does conform. The same is true for the {@link
 * ScoreScript}, which is also a {@link org.apache.lucene.search.DoubleValues}.
 *
 * @param <T> factory class type
 */
public class ScriptContext<T> {
  public final String name;
  public final Class<T> factoryClazz;
  public final Class<?> statefulFactoryClazz;
  public final Class<?> instanceClazz;

  /**
   * Create a script compile context with the given factory Class. Uses reflection to determine the
   * stateful factory and instance Classes. This constructor can only be used if the interface
   * Classes all conform with the script compile contract.
   *
   * @param name context name
   * @param factoryClazz factory class scripts of this type will compile to
   * @throws IllegalArgumentException if neither getInstance or getFactory are defined, if both
   *     getInstance and getFactory are defined, or if multiple getInstance or getFactory methods
   *     are defined
   */
  public ScriptContext(String name, Class<T> factoryClazz) {
    this.name = name;
    this.factoryClazz = factoryClazz;

    Method instanceMethod = getMethod(factoryClazz, "newInstance");
    Method factoryMethod = getMethod(factoryClazz, "newFactory");

    if (instanceMethod == null && factoryMethod == null) {
      throw new IllegalArgumentException(
          "Factory interface must define a newInstance or newFactory method");
    }
    if (instanceMethod != null && factoryMethod != null) {
      throw new IllegalArgumentException(
          "Factory interface cannot define both newInstance and newFactory methods");
    }

    if (instanceMethod != null) {
      statefulFactoryClazz = null;
      instanceClazz = instanceMethod.getReturnType();
    } else {
      statefulFactoryClazz = factoryMethod.getReturnType();
      Method statefulInstanceMethod = getMethod(statefulFactoryClazz, "newInstance");
      if (statefulInstanceMethod == null) {
        throw new IllegalArgumentException("Stateful factory must include newInstance method");
      }
      instanceClazz = statefulInstanceMethod.getReturnType();
    }
  }

  /**
   * Create a script compile context with the given type information. This constructor is used if
   * the types returned from the factory methods do not directly conform to the script compile
   * contract. Instead, subclasses which do conform must be provided.
   *
   * @param name context name
   * @param factoryClazz factory class scripts of this type will compile to
   * @param statefulFactoryClazz stateful factory that conforms with script compile contract, or
   *     null if stateful factory is not used
   * @param instanceClazz script instance that conforms with the script compile contract
   */
  public ScriptContext(
      String name, Class<T> factoryClazz, Class<?> statefulFactoryClazz, Class<?> instanceClazz) {
    this.name = name;
    this.factoryClazz = factoryClazz;
    this.statefulFactoryClazz = statefulFactoryClazz;
    this.instanceClazz = instanceClazz;
  }

  private Method getMethod(Class<?> clazz, String methodName) {
    List<Method> methods =
        Arrays.stream(clazz.getMethods())
            .filter(method -> method.getName().equals(methodName))
            .collect(Collectors.toList());
    if (methods.isEmpty()) {
      return null;
    }
    if (methods.size() != 1) {
      throw new IllegalArgumentException(
          "Expected at most 1 method named " + methodName + ", found " + methods.size());
    }
    return methods.get(0);
  }
}
