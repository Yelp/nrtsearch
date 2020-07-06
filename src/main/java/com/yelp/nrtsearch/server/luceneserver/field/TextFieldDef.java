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
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.luceneserver.field.properties.TermQueryable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/** Field class for 'TEXT' field type. */
public class TextFieldDef extends TextBaseFieldDef implements TermQueryable {
  public TextFieldDef(String name, Field requestField) {
    super(name, requestField);
  }

  @Override
  public String getType() {
    return "TEXT";
  }

  @Override
  public Query getTermQuery(TermQuery termQuery) {
    return new org.apache.lucene.search.TermQuery(new Term(getName(), termQuery.getTextValue()));
  }

  @Override
  public Query getTermInSetQuery(TermInSetQuery termInSetQuery) {
    List<BytesRef> textTerms =
        termInSetQuery.getTextTerms().getTermsList().stream()
            .map(BytesRef::new)
            .collect(Collectors.toList());
    return new org.apache.lucene.search.TermInSetQuery(termInSetQuery.getField(), textTerms);
  }
}
