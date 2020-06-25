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
package com.yelp.nrtsearch.server.luceneserver;

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
 * An {@link InputIterator} that pulls from a line file, using U+001f to join the suggestion, weight
 * and payload.
 */
public class FromFileTermFreqIterator implements InputIterator, Closeable {
  private final BufferedReader reader;
  private long weight;
  private int lineCount;
  final boolean hasContexts;

  /** How many suggestions were found. */
  public int suggestCount;

  private BytesRef extra;
  private final Set<BytesRef> contexts = new HashSet<>();

  /** Sole constructor. */
  public FromFileTermFreqIterator(File sourceFile, boolean hasContexts) throws IOException {
    reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(sourceFile), "UTF-8"), 65536);
    this.hasContexts = hasContexts;
  }

  @Override
  public boolean hasContexts() {
    return hasContexts;
  }

  @Override
  public Set<BytesRef> contexts() {
    return contexts;
  }

  @Override
  public boolean hasPayloads() {
    return true;
  }

  @Override
  public BytesRef next() {
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
      line = line.trim();
      if (line.length() == 0 || line.charAt(0) == '#') {
        continue;
      }
      int spot = line.indexOf('\u001f');
      if (spot == -1) {
        throw new RuntimeException("line " + lineCount + " is malformed");
      }
      weight = Long.parseLong(line.substring(0, spot));
      suggestCount++;
      int spot2 = line.indexOf('\u001f', spot + 1);
      if (spot2 == -1) {
        throw new RuntimeException("line " + lineCount + " is malformed");
      }
      BytesRef text = new BytesRef(line.substring(spot + 1, spot2));

      int spot3 = line.indexOf('\u001f', spot2 + 1);
      if (spot3 == -1) {
        extra = new BytesRef(line.substring(spot2 + 1));
      } else {
        extra = new BytesRef(line.substring(spot2 + 1, spot3));

        contexts.clear();

        int upto = spot3 + 1;
        while (true) {
          int nextUpto = line.indexOf('\u001f', upto);
          if (nextUpto == -1) {
            contexts.add(new BytesRef(line.substring(upto)));
            break;
          } else {
            contexts.add(new BytesRef(line.substring(upto, nextUpto)));
            upto = nextUpto + 1;
          }
        }
        // System.out.println("CONTEXTS: " + text.utf8ToString() + " -> " + contexts);
      }

      return text;
    }
  }

  @Override
  public BytesRef payload() {
    return extra;
  }

  @Override
  public long weight() {
    return weight;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
