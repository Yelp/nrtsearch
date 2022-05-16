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

import com.yelp.nrtsearch.server.grpc.BuildSuggestRequest;
import com.yelp.nrtsearch.server.grpc.BuildSuggestResponse;
import com.yelp.nrtsearch.server.grpc.SuggestLocalSource;
import java.io.File;
import java.io.IOException;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.util.BytesRef;

/* Updates existing suggestions, if the suggester supports near-real-time changes. */
public class UpdateSuggestHandler implements Handler<BuildSuggestRequest, BuildSuggestResponse> {
  @Override
  public BuildSuggestResponse handle(IndexState indexState, BuildSuggestRequest buildSuggestRequest)
      throws HandlerException {
    final String suggestName = buildSuggestRequest.getSuggestName();
    Lookup lookup = indexState.getSuggesters().get(suggestName);
    if (lookup == null) {
      throw new RuntimeException(
          "suggestName: this suggester (\""
              + suggestName
              + "\") was not yet built; valid suggestNames: "
              + indexState.getSuggesters().keySet());
    }
    if ((lookup instanceof AnalyzingInfixSuggester) == false) {
      throw new UnsupportedOperationException(
          "suggestName: can only update AnalyzingInfixSuggester; got " + lookup);
    }

    if (buildSuggestRequest.hasNonLocalSource()) {
      throw new UnsupportedOperationException(
          "Does not yet support pulling from index/expressions, like BuildSuggest");
    }
    if (!buildSuggestRequest.hasLocalSource()) {
      throw new UnsupportedOperationException("Only supports pulling suggestions from local file");
    }

    final AnalyzingInfixSuggester lookup2 = (AnalyzingInfixSuggester) lookup;
    SuggestLocalSource localSource = buildSuggestRequest.getLocalSource();
    final File localFile = new File(localSource.getLocalFile());
    final boolean hasContexts = localSource.getHasContexts();

    try {
      return updateSuggestions(localFile, hasContexts, lookup2);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private BuildSuggestResponse updateSuggestions(
      File localFile, boolean hasContexts, AnalyzingInfixSuggester lookup) throws IOException {
    InputIterator iterator = new FromFileTermFreqIterator(localFile, hasContexts);
    boolean hasPayloads = iterator.hasPayloads();
    int count = 0;
    while (true) {
      BytesRef term = iterator.next();
      if (term == null) {
        break;
      }
      lookup.update(
          term,
          iterator.hasContexts() ? iterator.contexts() : null,
          iterator.weight(),
          hasPayloads ? iterator.payload() : null);
      count++;
    }
    lookup.refresh();

    return BuildSuggestResponse.newBuilder().setCount(count).build();
  }
}
