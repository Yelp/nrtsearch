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
package com.yelp.nrtsearch.server.luceneserver.field;

import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.IndexPrefixes;
import com.yelp.nrtsearch.server.grpc.PrefixQuery;
import com.yelp.nrtsearch.server.luceneserver.field.properties.PrefixQueryable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;

/** Field class for 'TEXT' field type. */
public class TextFieldDef extends TextBaseFieldDef implements PrefixQueryable {
  protected PrefixFieldDef prefixFieldDef;
  private final Map<String, IndexableFieldDef> childFieldsWithPrefix;
  private static final int DEFAULT_MIN_CHARS = 2;
  private static final int DEFAULT_MAX_CHARS = 5;

  public TextFieldDef(String name, Field requestField) {
    super(name, requestField);
    if (requestField.hasIndexPrefixes()) {
      verifySearchable("Prefix query");
      int minChars =
          requestField.getIndexPrefixes().hasMinChars()
              ? requestField.getIndexPrefixes().getMinChars()
              : DEFAULT_MIN_CHARS;
      int maxChars =
          requestField.getIndexPrefixes().hasMaxChars()
              ? requestField.getIndexPrefixes().getMaxChars()
              : DEFAULT_MAX_CHARS;
      validatePrefix(minChars, maxChars);
      Field.Builder prefixFieldBuilder =
          Field.newBuilder()
              .setSearch(true)
              .setIndexPrefixes(
                  IndexPrefixes.newBuilder().setMinChars(minChars).setMaxChars(maxChars).build());

      if (requestField.hasAnalyzer()) {
        prefixFieldBuilder.setAnalyzer(requestField.getAnalyzer());
      }
      if (requestField.hasIndexAnalyzer()) {
        prefixFieldBuilder.setIndexAnalyzer(requestField.getIndexAnalyzer());
      }

      this.prefixFieldDef = new PrefixFieldDef(getName(), prefixFieldBuilder.build());

      Map<String, IndexableFieldDef> childFieldsMap = new HashMap<>(super.getChildFields());
      childFieldsMap.put(prefixFieldDef.getName(), prefixFieldDef);
      childFieldsWithPrefix = Collections.unmodifiableMap(childFieldsMap);
    } else {
      this.prefixFieldDef = null;
      childFieldsWithPrefix = super.getChildFields();
    }
  }

  @Override
  public Map<String, IndexableFieldDef> getChildFields() {
    return childFieldsWithPrefix;
  }

  @Override
  public String getType() {
    return "TEXT";
  }

  public PrefixFieldDef getPrefixFieldDef() {
    return prefixFieldDef;
  }

  public boolean hasPrefix() {
    return prefixFieldDef != null;
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    super.parseDocumentField(document, fieldValues, facetHierarchyPaths);

    if (hasPrefix() && !fieldValues.isEmpty()) {
      prefixFieldDef.parseDocumentField(document, fieldValues, facetHierarchyPaths);
    }
  }

  @Override
  public Query getPrefixQuery(
      PrefixQuery prefixQuery, MultiTermQuery.RewriteMethod rewriteMethod, boolean spanQuery) {
    verifySearchable("Prefix query");
    if (hasPrefix() && prefixFieldDef.accept(prefixQuery.getPrefix().length()) && !spanQuery) {
      Query query = prefixFieldDef.getPrefixQuery(prefixQuery, rewriteMethod);
      if (rewriteMethod == null
          || rewriteMethod == MultiTermQuery.CONSTANT_SCORE_REWRITE
          || rewriteMethod == MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE) {
        return new ConstantScoreQuery(query);
      }
      return query;
    }
    org.apache.lucene.search.PrefixQuery query =
        new org.apache.lucene.search.PrefixQuery(
            new Term(prefixQuery.getField(), prefixQuery.getPrefix()));
    query.setRewriteMethod(rewriteMethod);
    return query;
  }

  public void validatePrefix(int minChars, int maxChars) {
    if (minChars > maxChars) {
      throw new IllegalArgumentException(
          "min_chars [" + minChars + "] must be less than max_chars [" + maxChars + "]");
    }
    if (minChars < 1) {
      throw new IllegalArgumentException("min_chars [" + minChars + "] must be greater than zero");
    }
    if (maxChars >= 20) {
      throw new IllegalArgumentException("max_chars [" + maxChars + "] must be less than 20");
    }
  }

  protected void verifySearchable(String featureName) {
    if (!isSearchable()) {
      throw new IllegalStateException(
          featureName + " requires field to be searchable: " + getName());
    }
  }
}
