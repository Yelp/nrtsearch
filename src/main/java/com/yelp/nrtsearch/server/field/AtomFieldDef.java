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
package com.yelp.nrtsearch.server.field;

import static com.yelp.nrtsearch.server.analysis.AnalyzerCreator.hasAnalyzer;

import com.yelp.nrtsearch.server.field.properties.PrefixQueryable;
import com.yelp.nrtsearch.server.field.properties.RangeQueryable;
import com.yelp.nrtsearch.server.field.properties.Sortable;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.PrefixQuery;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.SortType;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

/** Field class for 'ATOM' field type. Uses {@link KeywordAnalyzer} for text analysis. */
public class AtomFieldDef extends TextBaseFieldDef
    implements Sortable, RangeQueryable, PrefixQueryable {
  private static final Analyzer keywordAnalyzer = new KeywordAnalyzer();

  public AtomFieldDef(
      String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
    super(name, requestField, context);
  }

  @Override
  protected void validateRequest(Field requestField) {
    if (hasAnalyzer(requestField)) {
      throw new IllegalArgumentException(
          "no analyzer allowed with atom (it's hardwired to KeywordAnalyzer internally)");
    }
  }

  @Override
  public String getType() {
    return "ATOM";
  }

  @Override
  public Object parseLastValue(String value) {
    return new BytesRef(value);
  }

  @Override
  protected void setSearchProperties(FieldType fieldType, Field requestField) {
    // TODO: make this configurable and default to true, this is hard to do with the
    // current grpc field type
    fieldType.setOmitNorms(true);
    fieldType.setTokenized(false);
    if (requestField.getSearch()) {
      setIndexOptions(requestField.getIndexOptions(), fieldType, IndexOptions.DOCS);
    }
  }

  @Override
  protected Analyzer parseIndexAnalyzer(Field requestField) {
    return keywordAnalyzer;
  }

  @Override
  protected Analyzer parseSearchAnalyzer(Field requestField) {
    return keywordAnalyzer;
  }

  @Override
  public Query getTermQueryFromTextValue(String textValue) {
    verifySearchable("Term query");
    return new org.apache.lucene.search.TermQuery(new Term(getName(), textValue));
  }

  @Override
  public Query getTermInSetQueryFromTextValues(List<String> textValues) {
    verifySearchable("Term in set query");
    List<BytesRef> textTerms = textValues.stream().map(BytesRef::new).collect(Collectors.toList());
    return new org.apache.lucene.search.TermInSetQuery(getName(), textTerms);
  }

  @Override
  public SortField getSortField(SortType type) {
    verifyDocValues("Sort field");
    if (docValuesType != DocValuesType.SORTED && docValuesType != DocValuesType.SORTED_SET) {
      throw new IllegalStateException(
          "Sort field requires SORTED or SORTED_SET doc values: " + getName());
    }
    SortField sortField;
    if (isMultiValue()) {
      sortField =
          new SortedSetSortField(
              getName(), type.getReverse(), SORTED_SET_TYPE_PARSER.apply(type.getSelector()));
    } else {
      sortField = new SortField(getName(), SortField.Type.STRING, type.getReverse());
    }

    boolean missingLast = type.getMissingLast();
    if (missingLast) {
      sortField.setMissingValue(SortField.STRING_LAST);
    } else {
      sortField.setMissingValue(SortField.STRING_FIRST);
    }
    return sortField;
  }

  @Override
  public Query getRangeQuery(RangeQuery rangeQuery) {
    verifySearchableOrDocValues("Range query");
    BytesRef lowerTerm =
        rangeQuery.getLower().isEmpty() ? null : new BytesRef(rangeQuery.getLower());
    BytesRef upperTerm =
        rangeQuery.getUpper().isEmpty() ? null : new BytesRef(rangeQuery.getUpper());
    if (isSearchable()) {
      return new TermRangeQuery(
          getName(),
          lowerTerm,
          upperTerm,
          !rangeQuery.getLowerExclusive(),
          !rangeQuery.getUpperExclusive());
    } else if (hasDocValues()
        && (docValuesType == DocValuesType.SORTED || docValuesType == DocValuesType.SORTED_SET)) {
      return SortedSetDocValuesField.newSlowRangeQuery(
          getName(),
          lowerTerm,
          upperTerm,
          !rangeQuery.getLowerExclusive(),
          !rangeQuery.getUpperExclusive());
    } else {
      throw new IllegalStateException(
          "Only SORTED or SORTED_SET doc values are supported for range queries: " + getName());
    }
  }

  @Override
  public Query getPrefixQuery(PrefixQuery prefixQuery, MultiTermQuery.RewriteMethod rewriteMethod) {
    return new org.apache.lucene.search.PrefixQuery(
        new Term(prefixQuery.getField(), prefixQuery.getPrefix()), rewriteMethod);
  }
}
