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

import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Bindable;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Sortable;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.SortField;

/**
 * Field definition for a Virtual field. Virtual fields are able to produce a double value for each
 * given index document.
 */
public class VirtualFieldDef extends FieldDef implements Bindable, Sortable {
  private final DoubleValuesSource valuesSource;
  private final IndexableFieldDef.FacetValueType facetValueType;

  /**
   * Field constructor.
   *
   * @param name name of field
   * @param valuesSource lucene value source used to produce field value from documents
   */
  public VirtualFieldDef(String name, DoubleValuesSource valuesSource) {
    super(name);
    this.valuesSource = valuesSource;
    // Since we have the DoublesValueSource on this field should always be able to produce
    // numeric_range facets on it
    this.facetValueType = IndexableFieldDef.FacetValueType.NUMERIC_RANGE;
  }

  /**
   * Get value source for this field.
   *
   * @return lucene value source used to produce field value from documents
   */
  public DoubleValuesSource getValuesSource() {
    return valuesSource;
  }

  @Override
  public String getType() {
    return "VIRTUAL";
  }

  /**
   * Get the facet value type for this field.
   *
   * @return field facet value type
   */
  @Override
  public IndexableFieldDef.FacetValueType getFacetValueType() {
    return facetValueType;
  }

  @Override
  public DoubleValuesSource getExpressionBinding() {
    return getValuesSource();
  }

  @Override
  public SortField getSortField(SortType type) {
    return getValuesSource().getSortField(type.getReverse());
  }
}
