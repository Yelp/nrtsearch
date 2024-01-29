/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.analysis;

import static org.apache.lucene.analysis.util.AnalysisSPILoader.newFactoryClassInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class MappingV2CharFilterFactoryTest {

  @Test
  public void testMappingFilterSingleRule() throws IOException {
    MappingV2CharFilterFactory factory = getFactory(",=>.");
    String output = getFiltered(factory, "this. is, a test, string");
    assertEquals("this. is. a test. string", output);
  }

  @Test
  public void testMappingFilterMultipleRules() throws IOException {
    MappingV2CharFilterFactory factory =
        getFactory(
            "'s=>|'S=>|&=>\\u0020and\\u0020|'N'=>\\u0020n\\u0020|/=>\\u0020|\"=>\\u0020|!=>");
    String output = getFiltered(factory, "a'sb'Sc&d'N'e'n'f/g\"h!i");
    assertEquals("abc and d n e'n'f g hi", output);
  }

  @Test
  public void testDifferentSeparator() throws IOException {
    MappingV2CharFilterFactory factory =
        getFactory(
            "'s=>,'S=>,&=>\\u0020and\\u0020,'N'=>\\u0020n\\u0020,/=>\\u0020,\"=>\\u0020,!=>",
            "[,]");
    String output = getFiltered(factory, "a'sb'Sc&d'N'e'n'f/g\"h!i");
    assertEquals("abc and d n e'n'f g hi", output);
  }

  @Test
  public void testNoMappings() {
    try {
      newFactoryClassInstance(MappingV2CharFilterFactory.class, new HashMap<>());
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Filter mappings must be specified", e.getMessage());
    }
  }

  private MappingV2CharFilterFactory getFactory(String mappings) {
    Map<String, String> params = new HashMap<>();
    params.put(MappingV2CharFilterFactory.MAPPINGS, mappings);
    return newFactoryClassInstance(MappingV2CharFilterFactory.class, params);
  }

  private MappingV2CharFilterFactory getFactory(String mappings, String pattern) {
    Map<String, String> params = new HashMap<>();
    params.put(MappingV2CharFilterFactory.MAPPINGS, mappings);
    params.put(MappingV2CharFilterFactory.SEPARATOR_PATTERN, pattern);
    return newFactoryClassInstance(MappingV2CharFilterFactory.class, params);
  }

  private String getFiltered(MappingV2CharFilterFactory factory, String inputStr)
      throws IOException {
    Reader input = new CharArrayReader(inputStr.toCharArray());
    char[] output = new char[256];
    try (Reader reader = factory.create(input)) {
      int size = reader.read(output);
      return new String(output, 0, size);
    }
  }
}
