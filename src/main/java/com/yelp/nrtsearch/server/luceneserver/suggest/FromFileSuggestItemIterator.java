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
package com.yelp.nrtsearch.server.luceneserver.suggest;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.util.BytesRef;

/**
 * An {@link InputIterator} that pulls from a line file, using U+001f to join the suggestion,
 * including suggest text, suffix-gram terms, weight, and payload.
 *
 * <p>The format is expected to be: suggest_textterm1term2term3weightpayloadcontext1context2
 */
public class FromFileSuggestItemIterator implements InputIterator, Closeable {

  // INFORMATION SEPARATOR ONE to separate fields in each line.
  public static final String FIELD_SEPARATOR = "\u001f";
  // INFORMATION SEPARATOR TWO to separate strings inside the field.
  public static final String IN_FIELD_SEPARATOR = "\u001e";

  private final BufferedReader reader;
  private final boolean hasContexts;
  private final boolean hasPayload;
  private final int fieldNum;

  private int lineCount;
  /** How many suggestions were found. */
  public int suggestCount;

  private final Set<BytesRef> searchTexts = new HashSet<>();
  private final Set<BytesRef> contexts = new HashSet<>();
  private BytesRef text;
  private long weight;
  private BytesRef payload;

  public FromFileSuggestItemIterator(File sourceFile, boolean hasContexts, boolean hasPayload)
      throws IOException {
    reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(sourceFile), "UTF-8"), 65536);
    this.hasContexts = hasContexts;
    this.hasPayload = hasPayload;
    this.suggestCount = 0;
    this.fieldNum = 3 + (hasContexts ? 1 : 0) + (hasPayload ? 1 : 0);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public long weight() {
    return weight;
  }

  @Override
  public BytesRef payload() {
    if (hasPayload) {
      return payload;
    }
    return null;
  }

  @Override
  public boolean hasPayloads() {
    return hasPayload;
  }

  @Override
  public Set<BytesRef> contexts() {
    if (hasContexts) {
      return contexts;
    }
    return null;
  }

  @Override
  public boolean hasContexts() {
    return this.hasContexts;
  }

  public Set<BytesRef> searchTexts() {
    return searchTexts;
  }

  @Override
  public BytesRef next() throws IOException {
    while (true) {
      String line;
      try {
        line = reader.readLine();
      } catch (IOException ioe) {
        throw new RuntimeException("readLine failed", ioe);
      }

      if (line == null) {
        return null;
      }
      lineCount++;
      if (parseLine(line)) {
        break;
      }
    }

    return text;
  }

  private boolean parseLine(String line) {
    line = line.trim();
    if (line.isEmpty()) {
      return false;
    }

    String[] fieldArray = line.split(FIELD_SEPARATOR);
    if (fieldArray.length != fieldNum) {
      throw new RuntimeException("line " + lineCount + " is malformed");
    }
    int itemIndex = 0;

    text = new BytesRef(fieldArray[itemIndex++]);
    searchTexts.clear();
    for (String searchTextString : fieldArray[itemIndex++].split(IN_FIELD_SEPARATOR)) {
      searchTexts.add(new BytesRef(searchTextString));
    }
    weight = Long.parseLong(fieldArray[itemIndex++]);
    if (hasPayload) {
      payload = new BytesRef(fieldArray[itemIndex++]);
    }
    if (hasContexts) {
      contexts.clear();
      for (String contextString : fieldArray[itemIndex].split(IN_FIELD_SEPARATOR)) {
        contexts.add(new BytesRef(contextString));
      }
    }
    return true;
  }
}
