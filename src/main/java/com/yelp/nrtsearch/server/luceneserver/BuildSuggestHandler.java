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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.config.IndexPreloadConfig;
import com.yelp.nrtsearch.server.grpc.BuildSuggestRequest;
import com.yelp.nrtsearch.server.grpc.BuildSuggestResponse;
import com.yelp.nrtsearch.server.grpc.FuzzySuggester;
import com.yelp.nrtsearch.server.grpc.OneHighlight;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SuggestLookupHighlight;
import com.yelp.nrtsearch.server.grpc.SuggestNonLocalSource;
import com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.luceneserver.suggest.CompletionInfixSuggester;
import com.yelp.nrtsearch.server.luceneserver.suggest.FuzzyInfixSuggester;
import com.yelp.nrtsearch.server.luceneserver.suggest.iterator.FromProtobufFileSuggestItemIterator;
import com.yelp.nrtsearch.server.luceneserver.suggest.iterator.SuggestDocumentDictionary;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.search.suggest.DocumentDictionary;
import org.apache.lucene.search.suggest.DocumentValueSourceDictionary;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester;
import org.apache.lucene.search.suggest.document.FuzzyCompletionQuery;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles {@code buildSuggest}. */
public class BuildSuggestHandler implements Handler<BuildSuggestRequest, BuildSuggestResponse> {
  private final ThreadPoolExecutor threadPoolExecutor;
  Logger logger = LoggerFactory.getLogger(BuildSuggestHandler.class);
  private final JsonParser jsonParser = new JsonParser();
  private final Gson gson = new Gson();

  /** Load all previously built suggesters. */
  public void load(IndexState indexState, JsonObject saveState) throws IOException {
    ShardState shardState = indexState.getShard(0);
    for (Map.Entry<String, JsonElement> ent : saveState.entrySet()) {
      String suggestName = ent.getKey();
      JsonObject buildSuggestRequestAsJsonObject = ent.getValue().getAsJsonObject();
      String jsonStr = gson.toJson(buildSuggestRequestAsJsonObject);
      BuildSuggestRequest.Builder buildSuggestRequestBuilder = BuildSuggestRequest.newBuilder();
      try {
        JsonFormat.parser().merge(jsonStr, buildSuggestRequestBuilder);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      BuildSuggestRequest buildSuggestRequest = buildSuggestRequestBuilder.build();
      Lookup suggester = getSuggester(indexState, buildSuggestRequest);

      if ((suggester instanceof AnalyzingInfixSuggester) == false) {
        // nocommit store suggesters 1 dir up:
        try (IndexInput in =
            shardState.origIndexDir.openInput("suggest." + suggestName, IOContext.DEFAULT)) {
          suggester.load(in);
        }
      }
      indexState.addSuggest(suggestName, buildSuggestRequestAsJsonObject);
    }
  }

  private Lookup getSuggester(IndexState indexState, BuildSuggestRequest buildSuggestRequest)
      throws IOException {
    String suggestName = buildSuggestRequest.getSuggestName();

    ShardState shardState = indexState.getShard(0);

    Lookup oldSuggester = indexState.getSuggesters().get(suggestName);
    if (oldSuggester != null && oldSuggester instanceof Closeable) {
      ((Closeable) oldSuggester).close();
      indexState.getSuggesters().remove(suggestName);
    }

    Analyzer indexAnalyzer;
    Analyzer queryAnalyzer;
    final Lookup suggester;

    if (buildSuggestRequest.hasFuzzySuggester()) {
      int options = 0;
      FuzzySuggester fuzzySuggester = buildSuggestRequest.getFuzzySuggester();
      if (fuzzySuggester.getAnalyzer() != null) {
        indexAnalyzer = queryAnalyzer = AnalyzerCreator.getStandardAnalyzer();
      } else {
        indexAnalyzer = AnalyzerCreator.getStandardAnalyzer();
        queryAnalyzer = AnalyzerCreator.getStandardAnalyzer();
      }
      if (indexAnalyzer == null) {
        throw new RuntimeException("analyzer analyzer or indexAnalyzer must be specified");
      }
      if (queryAnalyzer == null) {
        throw new RuntimeException("analyzer analyzer or queryAnalyzer must be specified");
      }

      if (fuzzySuggester.getPreserveSep()) {
        options |= AnalyzingSuggester.PRESERVE_SEP;
      }
      if (fuzzySuggester.getExactFirst()) {
        options |= AnalyzingSuggester.EXACT_FIRST;
      }
      // default values
      int maxSurfaceFormsPerAnalyzedForm =
          fuzzySuggester.getMaxSurfaceFormsPerAnalyzedForm() == 0
              ? 156
              : fuzzySuggester.getMaxSurfaceFormsPerAnalyzedForm();
      int maxGraphExpansions =
          fuzzySuggester.getMaxGraphExpansions() == 0 ? -1 : fuzzySuggester.getMaxGraphExpansions();
      int minFuzzyLength =
          fuzzySuggester.getMinFuzzyLength() == 0
              ? org.apache.lucene.search.suggest.analyzing.FuzzySuggester.DEFAULT_MIN_FUZZY_LENGTH
              : fuzzySuggester.getMinFuzzyLength();
      int nonFuzzyPrefix =
          fuzzySuggester.getNonFuzzyPrefix() == 0
              ? org.apache.lucene.search.suggest.analyzing.FuzzySuggester.DEFAULT_NON_FUZZY_PREFIX
              : fuzzySuggester.getNonFuzzyPrefix();
      int maxEdits =
          fuzzySuggester.getMaxEdits() == 0
              ? org.apache.lucene.search.suggest.analyzing.FuzzySuggester.DEFAULT_MAX_EDITS
              : fuzzySuggester.getMaxEdits();

      suggester =
          new org.apache.lucene.search.suggest.analyzing.FuzzySuggester(
              shardState.origIndexDir,
              "FuzzySuggester",
              indexAnalyzer,
              queryAnalyzer,
              options,
              maxSurfaceFormsPerAnalyzedForm,
              maxGraphExpansions,
              true,
              maxEdits,
              fuzzySuggester.getTranspositions(),
              nonFuzzyPrefix,
              minFuzzyLength,
              fuzzySuggester.getUnicodeAware());
    } else if (buildSuggestRequest.hasAnalyzingSuggester()) {
      int options = 0;
      com.yelp.nrtsearch.server.grpc.AnalyzingSuggester analyzingSuggester =
          buildSuggestRequest.getAnalyzingSuggester();
      if (analyzingSuggester.getAnalyzer() != null) {
        indexAnalyzer = queryAnalyzer = AnalyzerCreator.getStandardAnalyzer();
      } else {
        indexAnalyzer = AnalyzerCreator.getStandardAnalyzer();
        queryAnalyzer = AnalyzerCreator.getStandardAnalyzer();
      }
      if (indexAnalyzer == null) {
        throw new RuntimeException("analyzer analyzer or indexAnalyzer must be specified");
      }
      if (queryAnalyzer == null) {
        throw new RuntimeException("analyzer analyzer or queryAnalyzer must be specified");
      }

      if (analyzingSuggester.getPreserveSep()) {
        options |= AnalyzingSuggester.PRESERVE_SEP;
      }
      if (analyzingSuggester.getExactFirst()) {
        options |= AnalyzingSuggester.EXACT_FIRST;
      }
      // default values
      int maxSurfaceFormsPerAnalyzedForm =
          analyzingSuggester.getMaxSurfaceFormsPerAnalyzedForm() == 0
              ? 256
              : analyzingSuggester.getMaxSurfaceFormsPerAnalyzedForm();
      int maxGraphExpansions =
          analyzingSuggester.getMaxGraphExpansions() == 0
              ? -1
              : analyzingSuggester.getMaxGraphExpansions();
      suggester =
          new AnalyzingSuggester(
              shardState.origIndexDir,
              "AnalyzingSuggester",
              indexAnalyzer,
              queryAnalyzer,
              options,
              maxSurfaceFormsPerAnalyzedForm,
              maxGraphExpansions,
              true);
    } else if (buildSuggestRequest.hasInfixSuggester()) {
      if (buildSuggestRequest.getInfixSuggester().getAnalyzer() != null) {
        indexAnalyzer = queryAnalyzer = AnalyzerCreator.getStandardAnalyzer();
      } else {
        indexAnalyzer = AnalyzerCreator.getStandardAnalyzer();
        queryAnalyzer = AnalyzerCreator.getStandardAnalyzer();
      }
      if (indexAnalyzer == null) {
        throw new RuntimeException("analyzer analyzer or indexAnalyzer must be specified");
      }
      if (queryAnalyzer == null) {
        throw new RuntimeException("analyzer analyzer or queryAnalyzer must be specified");
      }

      suggester =
          new AnalyzingInfixSuggester(
              indexState
                  .getDirectoryFactory()
                  .open(
                      indexState.getRootDir().resolve("suggest." + suggestName + ".infix"),
                      IndexPreloadConfig.PRELOAD_ALL),
              indexAnalyzer,
              queryAnalyzer,
              AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS,
              true) {
            @Override
            protected Object highlight(String text, Set<String> matchedTokens, String prefixToken)
                throws IOException {

              // We override the entire highlight method, to
              // render directly to JSONArray instead of html
              // string:

              // nocommit push this fix (queryAnalyzer -> indexAnalyzer) back:
              TokenStream ts = indexAnalyzer.tokenStream("text", new StringReader(text));
              CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
              OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
              ts.reset();
              SuggestLookupHighlight.Builder suggestLookupHighlightBuilder =
                  SuggestLookupHighlight.newBuilder();
              int upto = 0;
              while (ts.incrementToken()) {
                String token = termAtt.toString();
                int startOffset = offsetAtt.startOffset();
                int endOffset = offsetAtt.endOffset();
                if (upto < startOffset) {
                  OneHighlight.Builder oneHighlightBuilder = OneHighlight.newBuilder();
                  oneHighlightBuilder.setIsHit(false);
                  oneHighlightBuilder.setText(text.substring(upto, startOffset));
                  suggestLookupHighlightBuilder.addOneHighlight(oneHighlightBuilder);
                  upto = startOffset;
                } else if (upto > startOffset) {
                  continue;
                }

                if (matchedTokens.contains(token)) {
                  // Token matches.
                  OneHighlight.Builder oneHighlightBuilder = OneHighlight.newBuilder();
                  oneHighlightBuilder.setIsHit(true);
                  oneHighlightBuilder.setText(text.substring(startOffset, endOffset));
                  suggestLookupHighlightBuilder.addOneHighlight(oneHighlightBuilder);
                  upto = endOffset;
                } else if (prefixToken != null && token.startsWith(prefixToken)) {
                  OneHighlight.Builder oneHighlightBuilder = OneHighlight.newBuilder();
                  oneHighlightBuilder.setIsHit(true);
                  oneHighlightBuilder.setText(
                      text.substring(startOffset, startOffset + prefixToken.length()));
                  suggestLookupHighlightBuilder.addOneHighlight(oneHighlightBuilder);
                  if (prefixToken.length() < token.length()) {
                    OneHighlight.Builder oneMoreHighlightBuilder = OneHighlight.newBuilder();
                    oneMoreHighlightBuilder.setIsHit(false);
                    oneMoreHighlightBuilder.setText(
                        text.substring(
                            startOffset + prefixToken.length(), startOffset + token.length()));
                    suggestLookupHighlightBuilder.addOneHighlight(oneMoreHighlightBuilder);
                  }
                  upto = endOffset;
                }
              }
              ts.end();
              int endOffset = offsetAtt.endOffset();
              if (upto < endOffset) {
                OneHighlight.Builder oneHighlightBuilder = OneHighlight.newBuilder();
                oneHighlightBuilder.setIsHit(false);
                oneHighlightBuilder.setText(text.substring(upto));
                suggestLookupHighlightBuilder.addOneHighlight(oneHighlightBuilder);
              }
              ts.close();

              return suggestLookupHighlightBuilder.build();
            }
          };
    } else if (buildSuggestRequest.hasCompletionInfixSuggester()) {
      com.yelp.nrtsearch.server.grpc.CompletionInfixSuggester completionInfixSuggester =
          buildSuggestRequest.getCompletionInfixSuggester();
      // Todo: use standard analyzer by default. Different analyzers will be considered later
      if (completionInfixSuggester.getAnalyzer() != null) {
        indexAnalyzer = queryAnalyzer = AnalyzerCreator.getStandardAnalyzer();
      } else {
        indexAnalyzer = AnalyzerCreator.getStandardAnalyzer();
        queryAnalyzer = AnalyzerCreator.getStandardAnalyzer();
      }
      if (indexAnalyzer == null) {
        throw new RuntimeException("analyzer analyzer or indexAnalyzer must be specified");
      }
      if (queryAnalyzer == null) {
        throw new RuntimeException("analyzer analyzer or queryAnalyzer must be specified");
      }

      suggester =
          new CompletionInfixSuggester(
              indexState
                  .getDirectoryFactory()
                  .open(
                      indexState.getRootDir().resolve("suggest." + suggestName + ".infix"),
                      IndexPreloadConfig.PRELOAD_ALL),
              indexAnalyzer,
              queryAnalyzer);
    } else if (buildSuggestRequest.hasFuzzyInfixSuggester()) {
      com.yelp.nrtsearch.server.grpc.FuzzyInfixSuggester fuzzyInfixSuggester =
          buildSuggestRequest.getFuzzyInfixSuggester();
      // Todo: use standard analyzer by default. Different analyzers will be considered later
      if (fuzzyInfixSuggester.getAnalyzer() != null) {
        indexAnalyzer = queryAnalyzer = AnalyzerCreator.getStandardAnalyzer();
      } else {
        indexAnalyzer = AnalyzerCreator.getStandardAnalyzer();
        queryAnalyzer = AnalyzerCreator.getStandardAnalyzer();
      }
      if (indexAnalyzer == null) {
        throw new RuntimeException("analyzer analyzer or indexAnalyzer must be specified");
      }
      if (queryAnalyzer == null) {
        throw new RuntimeException("analyzer analyzer or queryAnalyzer must be specified");
      }

      int maxEdits =
          fuzzyInfixSuggester.getMaxEdits() == 0
              ? FuzzyCompletionQuery.DEFAULT_MAX_EDITS
              : fuzzyInfixSuggester.getMaxEdits();
      boolean transpositions = fuzzyInfixSuggester.getTranspositions();
      int nonFuzzyPrefix =
          fuzzyInfixSuggester.getNonFuzzyPrefix() == 0
              ? FuzzyCompletionQuery.DEFAULT_NON_FUZZY_PREFIX
              : fuzzyInfixSuggester.getNonFuzzyPrefix();
      int minFuzzyLength =
          fuzzyInfixSuggester.getMinFuzzyLength() == 0
              ? FuzzyCompletionQuery.DEFAULT_MIN_FUZZY_LENGTH
              : fuzzyInfixSuggester.getMinFuzzyLength();
      boolean unicodeAware = fuzzyInfixSuggester.getUnicodeAware();

      suggester =
          new FuzzyInfixSuggester(
              indexState
                  .getDirectoryFactory()
                  .open(
                      indexState.getRootDir().resolve("suggest." + suggestName + ".infix"),
                      IndexPreloadConfig.PRELOAD_ALL),
              indexAnalyzer,
              queryAnalyzer,
              maxEdits,
              transpositions,
              nonFuzzyPrefix,
              minFuzzyLength,
              unicodeAware);
    } else {
      throw new RuntimeException(
          "Suggester provided must be one of AnalyzingSuggester, InfixSuggester, FuzzySuggester");
    }

    indexState.getSuggesters().put(suggestName, suggester);

    return suggester;
  }

  /** Used to return highlighted result; see {@link Lookup.LookupResult#highlightKey} */
  public static final class LookupHighlightFragment {
    /** Portion of text for this fragment. */
    public final String text;

    /** True if this text matched a part of the user's query. */
    public final boolean isHit;

    /** Sole constructor. */
    public LookupHighlightFragment(String text, boolean isHit) {
      this.text = text;
      this.isHit = isHit;
    }

    @Override
    public String toString() {
      return "LookupHighlightFragment(text=" + text + " isHit=" + isHit + ")";
    }
  }

  /** Wraps another {@link InputIterator} and counts how many suggestions were seen. */
  private static class CountingInputIterator implements InputIterator {

    private final InputIterator other;
    private int count;

    public CountingInputIterator(InputIterator other) {
      this.other = other;
    }

    @Override
    public boolean hasContexts() {
      return other.hasContexts();
    }

    @Override
    public Set<BytesRef> contexts() {
      return other.contexts();
    }

    @Override
    public long weight() {
      return other.weight();
    }

    @Override
    public BytesRef next() throws IOException {
      BytesRef result = other.next();
      if (result != null) {
        count++;
      }

      return result;
    }

    @Override
    public BytesRef payload() {
      return other.payload();
    }

    @Override
    public boolean hasPayloads() {
      return other.hasPayloads();
    }

    public int getCount() {
      return count;
    }
  }

  public BuildSuggestHandler(ThreadPoolExecutor threadPoolExecutor) {
    this.threadPoolExecutor = threadPoolExecutor;
  }

  @Override
  public BuildSuggestResponse handle(IndexState indexState, BuildSuggestRequest buildSuggestRequest)
      throws HandlerException {

    ShardState shardState = indexState.getShard(0);
    final JsonObject buildSuggestRequestAsJsonObject;
    try {
      // convert Proto object to Json String
      String jsonOrig = JsonFormat.printer().print(buildSuggestRequest);
      buildSuggestRequestAsJsonObject = jsonParser.parse(jsonOrig).getAsJsonObject();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    final String suggestName = buildSuggestRequest.getSuggestName();
    if (!IndexState.isSimpleName(suggestName)) {
      throw new RuntimeException(
          "suggestName: invalid suggestName \""
              + suggestName
              + "\": must be [a-zA-Z_][a-zA-Z0-9]*");
    }

    final Lookup suggester;
    try {
      suggester = getSuggester(indexState, buildSuggestRequest);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    InputIterator iterator = null;
    final SearcherTaxonomyManager.SearcherAndTaxonomy searcher;

    if (buildSuggestRequest.hasLocalSource()) {
      final File localFile = new File(buildSuggestRequest.getLocalSource().getLocalFile());
      if (!localFile.exists()) {
        throw new RuntimeException(
            String.format(
                "localFile %s does not exist",
                buildSuggestRequest.getLocalSource().getLocalFile()));
      }
      if (!localFile.canRead()) {
        throw new RuntimeException(
            String.format(
                "localFile %s cannot read file",
                buildSuggestRequest.getLocalSource().getLocalFile()));
      }
      boolean hasContexts = buildSuggestRequest.getLocalSource().getHasContexts();
      boolean hasPayload = buildSuggestRequest.getLocalSource().getHasPayload();
      boolean hasMultiSearchTexts = buildSuggestRequest.getLocalSource().getHasMultiSearchText();
      searcher = null;
      // Pull suggestions from local file:
      try {
        if (hasMultiSearchTexts) {
          iterator =
              new FromProtobufFileSuggestItemIterator(
                  localFile, hasContexts, hasPayload, hasMultiSearchTexts);
        } else {
          iterator = new FromFileTermFreqIterator(localFile, hasContexts);
        }
      } catch (IOException ioe) {
        throw new RuntimeException("localFile cannot open file", ioe);
      }
    } else {
      indexState.verifyStarted();
      SuggestNonLocalSource nonLocalSource = buildSuggestRequest.getNonLocalSource();
      // Pull suggestions from stored docs:
      SuggestNonLocalSource.WeightCase weightCase =
          buildSuggestRequest.getNonLocalSource().getWeightCase();
      SuggestNonLocalSource.SearcherCase searcherCase =
          buildSuggestRequest.getNonLocalSource().getSearcherCase();
      SearchRequest.Builder searchRequestBuilder = SearchRequest.newBuilder();
      if (searcherCase.equals(SuggestNonLocalSource.SearcherCase.INDEXGEN)) {
        searchRequestBuilder.setIndexGen(buildSuggestRequest.getNonLocalSource().getIndexGen());
      } else if (searcherCase.equals(SuggestNonLocalSource.SearcherCase.VERSION)) {
        searchRequestBuilder.setVersion(buildSuggestRequest.getNonLocalSource().getVersion());
      } else if (searcherCase.equals(SuggestNonLocalSource.SearcherCase.SNAPSHOT)) {
        searchRequestBuilder.setSnapshot(buildSuggestRequest.getNonLocalSource().getSnapshot());
      } else {
        logger.info("using current searcher version to build suggest index");
      }

      try {
        if (!searcherCase.equals(SuggestNonLocalSource.SearcherCase.SEARCHER_NOT_SET)) {
          // Specific searcher version:
          searcher =
              SearchHandler.getSearcherAndTaxonomy(
                  searchRequestBuilder.build(), indexState, shardState, null, threadPoolExecutor);
        } else {
          searcher = shardState.acquire();
        }
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }

      String suggestField = nonLocalSource.getSuggestField();

      String payloadField = nonLocalSource.getPayloadField();
      String contextField = nonLocalSource.getContextField();
      String searchTextField = nonLocalSource.getSearchTextField();

      DocumentDictionary dict;

      if (weightCase.equals(SuggestNonLocalSource.WeightCase.WEIGHTFIELD)) {
        // Weight is a field
        String weightField = nonLocalSource.getWeightField();
        if (!payloadField.isEmpty() && !contextField.isEmpty() && !searchTextField.isEmpty()) {
          dict =
              new SuggestDocumentDictionary(
                  searcher.searcher.getIndexReader(),
                  suggestField,
                  weightField,
                  payloadField,
                  contextField,
                  searchTextField);
        } else if (contextField.isEmpty()) {
          dict =
              new DocumentDictionary(
                  searcher.searcher.getIndexReader(), suggestField, weightField, payloadField);
        } else {
          dict =
              new DocumentDictionary(
                  searcher.searcher.getIndexReader(),
                  suggestField,
                  weightField,
                  payloadField,
                  contextField);
        }
      } else {
        // Weight is an expression; add bindings for all
        // numeric DV fields:
        Expression expr;
        try {
          expr = JavascriptCompiler.compile(nonLocalSource.getWeightExpression());
        } catch (Exception e) {
          throw new RuntimeException("weightExpression: expression does not compile", e);
        }

        if (contextField.isEmpty()) {
          dict =
              new DocumentValueSourceDictionary(
                  searcher.searcher.getIndexReader(),
                  suggestField,
                  expr.getDoubleValuesSource(indexState.getExpressionBindings())
                      .toLongValuesSource(),
                  payloadField);
        } else {
          dict =
              new DocumentValueSourceDictionary(
                  searcher.searcher.getIndexReader(),
                  suggestField,
                  expr.getDoubleValuesSource(indexState.getExpressionBindings())
                      .toLongValuesSource(),
                  payloadField,
                  contextField);
        }
      }

      try {
        iterator = dict.getEntryIterator();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    // nocommit return error if suggester already exists?
    // nocommit need a DeleteSuggestHandler
    try {
      return getBuildSuggestResponse(
          suggester,
          indexState,
          shardState,
          iterator,
          suggestName,
          buildSuggestRequestAsJsonObject,
          searcher);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private BuildSuggestResponse getBuildSuggestResponse(
      Lookup suggester,
      IndexState indexState,
      ShardState shardState,
      InputIterator iterator,
      String suggestName,
      JsonObject buildSuggestRequestAsJsonObject,
      SearcherTaxonomyManager.SearcherAndTaxonomy searcher)
      throws IOException {
    final InputIterator iterator0 = iterator;
    try {
      suggester.build(iterator0);
    } catch (IOException e) {
      logger.info("error while building suggest index");
      throw new RuntimeException(e);
    } finally {
      if (iterator0 instanceof Closeable) {
        ((Closeable) iterator0).close();
      }
      if (searcher != null) {
        shardState.release(searcher);
      }
    }

    boolean success = false;
    try (IndexOutput out =
        shardState.origIndexDir.createOutput("suggest." + suggestName, IOContext.DEFAULT)) {
      // nocommit look @ return value
      suggester.store(out);
      success = true;
    } finally {
      if (success == false) {
        shardState.origIndexDir.deleteFile("suggest." + suggestName);
      }
    }

    indexState.addSuggest(suggestName, buildSuggestRequestAsJsonObject);

    BuildSuggestResponse.Builder buildSuggestResponseBuilder = BuildSuggestResponse.newBuilder();
    if (suggester instanceof AnalyzingSuggester) {
      buildSuggestResponseBuilder.setSizeInBytes(((AnalyzingSuggester) suggester).ramBytesUsed());
    }
    if (suggester instanceof AnalyzingInfixSuggester) {
      ((AnalyzingInfixSuggester) suggester).commit();
    }

    buildSuggestResponseBuilder.setCount(suggester.getCount());
    return buildSuggestResponseBuilder.build();
  }
}
