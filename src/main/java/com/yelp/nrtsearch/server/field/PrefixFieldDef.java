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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
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
  private static final String INDEX_PREFIX = "._index_prefix";

  public PrefixFieldDef(
      String parentName, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
    super(parentName + INDEX_PREFIX, requestField, context);
    this.minChars = requestField.getIndexPrefixes().getMinChars();
    this.maxChars = requestField.getIndexPrefixes().getMaxChars();
    this.parentField = parentName;
  }

  @Override
  protected void setSearchProperties(FieldType fieldType, Field requestField) {
    fieldType.setOmitNorms(true);
    fieldType.setTokenized(true);
    fieldType.setIndexOptions(IndexOptions.DOCS);
  }

  @Override
  protected Analyzer parseIndexAnalyzer(Field requestField) {
    Analyzer baseAnalyzer = super.parseIndexAnalyzer(requestField);
    if (baseAnalyzer == null) {
      throw new IllegalArgumentException("Could not determine analyzer");
    }
    return new PrefixWrappedAnalyzer(
        baseAnalyzer,
        requestField.getIndexPrefixes().getMinChars(),
        requestField.getIndexPrefixes().getMaxChars());
  }

  boolean accept(int length) {
    return length >= minChars - 1 && length <= maxChars;
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

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), minChars, maxChars, parentField);
  }
}
