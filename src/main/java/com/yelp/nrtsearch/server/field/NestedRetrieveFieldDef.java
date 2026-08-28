/*
 * Copyright 2026 Yelp Inc.
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

/**
 * Marker FieldDef wrapper used for retrieveFields with {@code _CHILDREN.} or {@code _PARENT.}
 * prefix. Carries the direction of navigation (PARENT or CHILDREN), the underlying field to
 * retrieve values from, and the actual field name without the prefix.
 *
 * <p>This is used by the SearchHandler's field retrieval logic to detect when a field requires
 * navigating to a parent or child document rather than reading directly from the hit document.
 */
public class NestedRetrieveFieldDef extends FieldDef {

  /** Direction of nested navigation for field retrieval. */
  public enum Direction {
    /** Navigate from parent hit to child documents (collect child values). */
    CHILDREN,
    /** Navigate from child hit to parent document (read parent values). */
    PARENT
  }

  private final Direction direction;
  private final IndexableFieldDef<?> underlyingField;
  private final String actualFieldName;

  /**
   * Constructor.
   *
   * @param prefixedName the field name as specified in the request (with prefix)
   * @param direction the navigation direction
   * @param underlyingField the actual field definition to read values from
   * @param actualFieldName the field name without the _CHILDREN. or _PARENT. prefix
   */
  public NestedRetrieveFieldDef(
      String prefixedName,
      Direction direction,
      IndexableFieldDef<?> underlyingField,
      String actualFieldName) {
    super(prefixedName);
    this.direction = direction;
    this.underlyingField = underlyingField;
    this.actualFieldName = actualFieldName;
  }

  public Direction getDirection() {
    return direction;
  }

  public IndexableFieldDef<?> getUnderlyingField() {
    return underlyingField;
  }

  public String getActualFieldName() {
    return actualFieldName;
  }

  @Override
  public String getType() {
    return "NESTED_RETRIEVE";
  }

  @Override
  public IndexableFieldDef.FacetValueType getFacetValueType() {
    return IndexableFieldDef.FacetValueType.NO_FACETS;
  }
}
