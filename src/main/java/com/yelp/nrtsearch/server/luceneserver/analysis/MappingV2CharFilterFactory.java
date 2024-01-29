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

import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.util.CharFilterFactory;

/**
 * Implementation of a {@link CharFilterFactory} that allows for specification of character mapping
 * rules. Unlike the lucene provided {@link
 * org.apache.lucene.analysis.charfilter.MappingCharFilterFactory}, this one lets you specify the
 * mapping rules inline as a parameter (instead of within a file).
 *
 * <p>Rules must be specified in the 'mappings' parameter string. This value is separated into
 * multiple rules by splitting on a pattern, which defaults to '[|]'. This pattern may be changed by
 * giving a 'separator_pattern' param.
 *
 * <p>Rules must be of the form chars_from=>chars_to for example:
 *
 * <p>i=>j
 *
 * <p>,=>
 *
 * <p>&=>and
 *
 * <p>j=>k|a=>e
 *
 * <p>For escaping options, see {@link #parseString(String)}.
 */
public class MappingV2CharFilterFactory extends CharFilterFactory {

  /** SPI name */
  public static final String NAME = "mappingV2";

  public static final String SEPARATOR_PATTERN = "separator_pattern";
  public static final String DEFAULT_SEPARATOR_PATTERN = "[|]";
  public static final String MAPPINGS = "mappings";

  protected NormalizeCharMap normMap;

  /**
   * Initialize this factory via a set of key-value pairs.
   *
   * @param args
   */
  public MappingV2CharFilterFactory(Map<String, String> args) {
    super(args);

    String separator = args.getOrDefault(SEPARATOR_PATTERN, DEFAULT_SEPARATOR_PATTERN);
    String mappings = args.get(MAPPINGS);
    if (mappings == null) {
      throw new IllegalArgumentException("Filter mappings must be specified");
    }
    List<String> rules = extractRules(mappings, separator);
    final NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
    parseRules(rules, builder);
    normMap = builder.build();
  }

  @Override
  public Reader create(Reader input) {
    return new MappingCharFilter(normMap, input);
  }

  @Override
  public Reader normalize(Reader input) {
    return create(input);
  }

  private List<String> extractRules(String mappings, String separator) {
    return Arrays.asList(mappings.split(separator));
  }

  static Pattern p = Pattern.compile("(.*)\\s*=>\\s*(.*)\\s*$");

  protected void parseRules(List<String> rules, NormalizeCharMap.Builder builder) {
    for (String rule : rules) {
      Matcher m = p.matcher(rule);
      if (!m.find()) throw new IllegalArgumentException("Invalid Mapping Rule : [" + rule + "]");
      builder.add(parseString(m.group(1)), parseString(m.group(2)));
    }
  }

  char[] out = new char[256];

  protected String parseString(String s) {
    int readPos = 0;
    int len = s.length();
    int writePos = 0;
    while (readPos < len) {
      char c = s.charAt(readPos++);
      if (c == '\\') {
        if (readPos >= len)
          throw new IllegalArgumentException("Invalid escaped char in [" + s + "]");
        c = s.charAt(readPos++);
        switch (c) {
          case '\\':
            c = '\\';
            break;
          case '"':
            c = '"';
            break;
          case 'n':
            c = '\n';
            break;
          case 't':
            c = '\t';
            break;
          case 'r':
            c = '\r';
            break;
          case 'b':
            c = '\b';
            break;
          case 'f':
            c = '\f';
            break;
          case 'u':
            if (readPos + 3 >= len)
              throw new IllegalArgumentException("Invalid escaped char in [" + s + "]");
            c = (char) Integer.parseInt(s.substring(readPos, readPos + 4), 16);
            readPos += 4;
            break;
        }
      }
      out[writePos++] = c;
    }
    return new String(out, 0, writePos);
  }
}
