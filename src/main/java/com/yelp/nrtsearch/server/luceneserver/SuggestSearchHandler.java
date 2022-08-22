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

import com.yelp.nrtsearch.server.grpc.OneSuggestSearchResponse;
import com.yelp.nrtsearch.server.grpc.SuggestSearchRequest;
import com.yelp.nrtsearch.server.grpc.SuggestSearchResponse;
import com.yelp.nrtsearch.server.luceneserver.field.ContextSuggestFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.ContextQuery;
import org.apache.lucene.search.suggest.document.FuzzyCompletionQuery;
import org.apache.lucene.search.suggest.document.PrefixCompletionQuery;
import org.apache.lucene.search.suggest.document.SuggestIndexSearcher;
import org.apache.lucene.search.suggest.document.TopSuggestDocs;

public class SuggestSearchHandler implements Handler<SuggestSearchRequest, SuggestSearchResponse> {
  @Override
  public SuggestSearchResponse handle(
      IndexState indexState, SuggestSearchRequest suggestLookupRequest) throws HandlerException {
    ShardState shardState = indexState.getShard(0);
    indexState.verifyStarted();
    IndexReader indexReader;
    try {
      indexReader = shardState.acquire().searcher.getIndexReader();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    FieldDef fieldDef = indexState.getAllFields().get(suggestLookupRequest.getSuggestField());
    if (!(fieldDef instanceof ContextSuggestFieldDef)) {
      throw new IllegalArgumentException(
          String.format(
              "suggestField %s is not ContextSuggestField",
              suggestLookupRequest.getSuggestField()));
    }

    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(indexReader);

    CompletionQuery query = this.generateQuery(indexState, suggestLookupRequest);
    TopSuggestDocs suggest;
    try {
      suggest = suggestIndexSearcher.suggest(query, suggestLookupRequest.getCount(), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Set<OneSuggestSearchResponse> results =
        Arrays.stream(suggest.scoreLookupDocs())
            .map(mapScoreDocToOneSuggestSearchResponse(suggestLookupRequest, indexReader))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

    return SuggestSearchResponse.newBuilder().addAllResults(results).build();
  }

  /**
   * Method to process {@link TopSuggestDocs.SuggestScoreDoc} to response TODO: add support for
   * retrieveFields in {@link SuggestSearchRequest}
   *
   * @param indexReader index reader used to retrieve documents
   * @return a map function that takes a {@link TopSuggestDocs.SuggestScoreDoc} and generates {@link
   *     OneSuggestSearchResponse}
   */
  private Function<TopSuggestDocs.SuggestScoreDoc, OneSuggestSearchResponse>
      mapScoreDocToOneSuggestSearchResponse(
          SuggestSearchRequest suggestLookupRequest, IndexReader indexReader) {
    return scoreDoc -> {
      try {
        Document document = indexReader.document(scoreDoc.doc);
        return OneSuggestSearchResponse.newBuilder()
            .setKey(scoreDoc.key.toString())
            .setScore(Math.round(scoreDoc.score))
            .setPayload(getPayloadFromDocument(suggestLookupRequest, document))
            .build();
      } catch (IOException e) {
        return null;
      }
    };
  }

  private String getPayloadFromDocument(SuggestSearchRequest suggestLookupRequest, Document doc) {
    IndexableField payload = doc.getField(suggestLookupRequest.getPayloadField());
    if (payload != null) {
      if (payload.binaryValue() != null) {
        return payload.binaryValue().utf8ToString();
      } else if (payload.stringValue() != null) {
        return payload.stringValue();
      }
    }
    return "";
  }

  private CompletionQuery generateQuery(
      IndexState indexState, SuggestSearchRequest suggestLookupRequest) {
    CompletionQuery completionQuery;
    switch (suggestLookupRequest.getSuggestQuery()) {
      case PREFIX:
        completionQuery =
            new PrefixCompletionQuery(
                indexState.searchAnalyzer,
                new Term(suggestLookupRequest.getSuggestField(), suggestLookupRequest.getText()));
        break;
      case FUZZY:
        completionQuery =
            new FuzzyCompletionQuery(
                indexState.searchAnalyzer,
                new Term(suggestLookupRequest.getSuggestField(), suggestLookupRequest.getText()));
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "suggestQuery %s is not supported", suggestLookupRequest.getSuggestQuery()));
    }

    // wrap in context query and add contexts
    ContextQuery contextQuery = new ContextQuery(completionQuery);
    suggestLookupRequest.getContextsList().forEach(s -> contextQuery.addContext(s));
    return contextQuery;
  }
}
