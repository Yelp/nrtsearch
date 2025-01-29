/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.field;

import com.yelp.nrtsearch.server.analysis.PrefixWrappedAnalyzer;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.PrefixQuery;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;

public class PrefixFieldDef extends TextBaseFieldDef {
  private final int minChars;
  private final int maxChars;
  private final String parentField;
  private final Analyzer indexAnalyzer;
  private final String name;
  private static final int DEFAULT_MIN_CHARS = 2;
  private static final int DEFAULT_MAX_CHARS = 5;

  public PrefixFieldDef(
      String parentField,
      String name,
      Field requestField,
      FieldDefCreator.FieldDefCreatorContext context) {
    super(name, requestField, context);
    int minChars =
        requestField.getIndexPrefixes().hasMinChars()
            ? requestField.getIndexPrefixes().getMinChars()
            : DEFAULT_MIN_CHARS;
    int maxChars =
        requestField.getIndexPrefixes().hasMaxChars()
            ? requestField.getIndexPrefixes().getMaxChars()
            : DEFAULT_MAX_CHARS;
    validatePrefix(minChars, maxChars);
    this.name = name;
    this.minChars = minChars;
    this.maxChars = maxChars;
    this.parentField = parentField;
    this.indexAnalyzer = parseIndexAnalyzer(requestField);
  }

  @Override
  protected void setSearchProperties(FieldType fieldType, Field requestField) {
    fieldType.setOmitNorms(true);
    fieldType.setIndexOptions(IndexOptions.DOCS);
    fieldType.setTokenized(true);
  }

  protected Analyzer parseIndexAnalyzer(Field requestField) {
    Analyzer baseAnalyzer = super.parseIndexAnalyzer(requestField);
    if (baseAnalyzer == null) {
      return null;
    }
    return new PrefixWrappedAnalyzer(baseAnalyzer, minChars, maxChars);
  }

  boolean accept(int length) {
    return length >= minChars - 1 && length <= maxChars;
  }

  public Optional<Analyzer> getIndexAnalyzer() {
    return Optional.ofNullable(indexAnalyzer);
  }

  protected void parseDocumentField(Document document, List<String> fieldValues) {

    String value = fieldValues.getFirst();
    // doubt here. How do I generate tokens or add them directly? Any particular type ?
    try (TokenStream tokenStream = indexAnalyzer.tokenStream(name, value)) {
      CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
      tokenStream.reset();

      while (tokenStream.incrementToken()) {
        document.add(new org.apache.lucene.document.Field(name, termAtt.toString(), fieldType));
      }
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Error analyzing prefix field value for field: %s", name), e);
    }
  }

  public Query getPrefixQuery(PrefixQuery prefixQuery) {
    String textValue = prefixQuery.getPrefix();
    if (textValue.length() >= minChars) {
      return super.getTermQueryFromTextValue(textValue);
    }
    List<Automaton> automata = new ArrayList<>();
    automata.add(Automata.makeString(textValue));
    for (int i = textValue.length(); i < minChars; i++) {
      automata.add(Automata.makeAnyChar());
    }
    Automaton automaton = Operations.concatenate(automata);
    AutomatonQuery query = new AutomatonQuery(new Term(getName(), textValue + "*"), automaton);

    return new BooleanQuery.Builder()
        .add(query, BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term(parentField, textValue)), BooleanClause.Occur.SHOULD)
        .build();
  }

  @Override
  public String getType() {
    return "PREFIX";
  }

  public int getMinChars() {
    return minChars;
  }

  public int getMaxChars() {
    return maxChars;
  }

  public void validatePrefix(int minChars, int maxChars) {
    if (minChars > maxChars) {
      throw new IllegalArgumentException(
          "min_chars [" + minChars + "] must be less than max_chars [" + maxChars + "]");
    }
    if (minChars < 2) {
      throw new IllegalArgumentException("min_chars [" + minChars + "] must be greater than zero");
    }
    if (maxChars >= 20) {
      throw new IllegalArgumentException("max_chars [" + maxChars + "] must be less than 20");
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), minChars, maxChars, parentField);
  }
}
