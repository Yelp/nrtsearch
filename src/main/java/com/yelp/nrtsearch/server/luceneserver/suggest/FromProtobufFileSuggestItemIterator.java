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

import com.yelp.nrtsearch.server.grpc.NrtsearchIndex;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.util.BytesRef;

/**
 * An {@link org.apache.lucene.search.suggest.InputIterator} that reads the binary file and parse
 * the suggest index information based on the `NrtsearchIndex` proto
 */
public class FromProtobufFileSuggestItemIterator implements InputIterator, Closeable {

  /** How many suggestions were found. */
  public int suggestCount;

  private final FileInputStream sourceStream;
  private final boolean hasContexts;
  private final boolean hasPayload;

  private final Set<BytesRef> searchTexts = new HashSet<>();
  private final Set<BytesRef> contexts = new HashSet<>();
  private BytesRef text;
  private long weight;
  private BytesRef payload;

  public FromProtobufFileSuggestItemIterator(
      File sourceFile, boolean hasContexts, boolean hasPayload) throws IOException {
    this.sourceStream = new FileInputStream(sourceFile);
    this.hasPayload = hasPayload;
    this.hasContexts = hasContexts;
    this.suggestCount = 0;
  }

  @Override
  public void close() throws IOException {
    sourceStream.close();
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
    NrtsearchIndex suggestIndexInfo = NrtsearchIndex.parseDelimitedFrom(sourceStream);
    if (suggestIndexInfo == null) {
      return null;
    }

    parseProtobufIndex(suggestIndexInfo);
    suggestCount++;
    return text;
  }

  /** Parse each protobuf object based on NrtsearchIndex proto schema */
  private boolean parseProtobufIndex(NrtsearchIndex suggestIndexInfo) {
    text = new BytesRef(String.valueOf(suggestIndexInfo.getUniqueId()));
    searchTexts.clear();
    if (suggestIndexInfo.getSearchTextsCount() > 0) {
      for (String searchTextString : suggestIndexInfo.getSearchTextsList()) {
        searchTexts.add(new BytesRef(searchTextString));
      }
    }
    weight = suggestIndexInfo.getScore();
    if (hasPayload) {
      payload = new BytesRef(suggestIndexInfo.getPayload().toByteArray());
    }
    contexts.clear();
    if (hasContexts && suggestIndexInfo.getContextsCount() > 0) {
      for (String contextString : suggestIndexInfo.getContextsList()) {
        contexts.add(new BytesRef(contextString));
      }
    }
    return true;
  }
}
