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

import com.yelp.nrtsearch.server.field.IndexableFieldDef.FacetValueType;
import com.yelp.nrtsearch.server.script.RuntimeScript;

/**
 * Field definition for a runtime field. Runtime fields are able to produce a value for each given
 * index document.
 */
public class RuntimeFieldDef extends FieldDef {
  private final RuntimeScript.SegmentFactory segmentFactory;
  private final IndexableFieldDef.FacetValueType facetValueType;

  /**
   * Field constructor.
   *
   * @param name name of field
   * @param segmentFactory lucene value source used to produce field value from documents
   */
  public RuntimeFieldDef(String name, RuntimeScript.SegmentFactory segmentFactory) {
    super(name);
    this.segmentFactory = segmentFactory;
    this.facetValueType = FacetValueType.NO_FACETS;
  }

  /**
   * Get value source for this field.
   *
   * @return Segment factory to create the expression.
   */
  public RuntimeScript.SegmentFactory getSegmentFactory() {
    return segmentFactory;
  }

  @Override
  public String getType() {
    return "RUNTIME";
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
}
