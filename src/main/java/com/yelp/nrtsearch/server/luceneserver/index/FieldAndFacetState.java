/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.index;

import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefBindings;
import com.yelp.nrtsearch.server.luceneserver.field.IdFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.ObjectFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.TextBaseFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.GlobalOrdinalable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetsConfig.DimConfig;

/**
 * Class to hold the current immutable state of index fields and facets. This includes the {@link
 * Field} messages used to register the fields, and any generated state such as the {@link FieldDef}
 * and {@link FacetsConfig}.
 */
public class FieldAndFacetState {
  // field
  private final Map<String, FieldDef> fields;
  private final boolean hasNestedChildFields;
  private final IdFieldDef idFieldDef;
  private final List<String> indexedAnalyzedFields;
  private final Map<String, FieldDef> eagerGlobalOrdinalFields;
  private final Map<String, GlobalOrdinalable> eagerFieldGlobalOrdinalFields;
  private final Bindings exprBindings;

  // facet
  private final FacetsConfig facetsConfig;
  private final Set<String> internalFacetFieldNames;

  /** Constructor for new state with default (empty) state. */
  public FieldAndFacetState() {
    fields = Collections.emptyMap();
    hasNestedChildFields = false;
    idFieldDef = null;
    indexedAnalyzedFields = Collections.emptyList();
    eagerGlobalOrdinalFields = Collections.emptyMap();
    eagerFieldGlobalOrdinalFields = Collections.emptyMap();
    exprBindings = new FieldDefBindings(fields);

    facetsConfig = new FacetsConfig();
    internalFacetFieldNames = Collections.emptySet();
  }

  /**
   * Constructor to initialize from state builder.
   *
   * @param builder state builder
   */
  FieldAndFacetState(Builder builder) {
    fields = Collections.unmodifiableMap(builder.fields);
    hasNestedChildFields = builder.hasNestedChildFields;
    idFieldDef = builder.idFieldDef;
    indexedAnalyzedFields = Collections.unmodifiableList(builder.indexedAnalyzedFields);
    eagerGlobalOrdinalFields = Collections.unmodifiableMap(builder.eagerGlobalOrdinalFields);
    eagerFieldGlobalOrdinalFields =
        Collections.unmodifiableMap(builder.eagerFieldGlobalOrdinalFields);
    exprBindings = builder.exprBindings;

    facetsConfig = builder.facetsConfig;
    internalFacetFieldNames = Collections.unmodifiableSet(builder.internalFacetFieldNames);
  }

  /** Get all index {@link FieldDef}s. */
  public Map<String, FieldDef> getFields() {
    return fields;
  }

  /** Get if the index has any nested child fields registered. */
  public boolean getHasNestedChildFields() {
    return hasNestedChildFields;
  }

  /** Get the index _ID field, if one is registered. */
  public Optional<IdFieldDef> getIdFieldDef() {
    return Optional.ofNullable(idFieldDef);
  }

  /** Get all field names that are text analyzed. */
  public List<String> getIndexedAnalyzedFields() {
    return indexedAnalyzedFields;
  }

  /** Get all fields with eager global ordinals enabled. */
  public Map<String, FieldDef> getEagerGlobalOrdinalFields() {
    return eagerGlobalOrdinalFields;
  }

  /** Get all fields with eager global ordinals enabled for doc values. */
  public Map<String, GlobalOrdinalable> getFieldEagerGlobalOrdinalFields() {
    return eagerFieldGlobalOrdinalFields;
  }

  /** Get field expression {@link Bindings} used for js scripting language. */
  public Bindings getExprBindings() {
    return exprBindings;
  }

  /** Get facet config. */
  public FacetsConfig getFacetsConfig() {
    return facetsConfig;
  }

  /** Get internal names for document facet fields. */
  public Set<String> getInternalFacetFieldNames() {
    return internalFacetFieldNames;
  }

  /** Get a builder initialized with the current state of this instance. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  public static class Builder {
    // field
    private final Map<String, FieldDef> fields;
    private boolean hasNestedChildFields;
    private IdFieldDef idFieldDef;
    private final List<String> indexedAnalyzedFields;
    private final Map<String, FieldDef> eagerGlobalOrdinalFields;
    private final Map<String, GlobalOrdinalable> eagerFieldGlobalOrdinalFields;
    private final Bindings exprBindings;

    // facet
    private final FacetsConfig facetsConfig;
    private final Set<String> internalFacetFieldNames;

    private Builder(FieldAndFacetState initial) {
      this.fields = new HashMap<>(initial.fields);
      this.hasNestedChildFields = initial.hasNestedChildFields;
      this.idFieldDef = initial.idFieldDef;
      this.indexedAnalyzedFields = new ArrayList<>(initial.indexedAnalyzedFields);
      this.eagerGlobalOrdinalFields = new HashMap<>(initial.eagerGlobalOrdinalFields);
      this.eagerFieldGlobalOrdinalFields = new HashMap<>(initial.eagerFieldGlobalOrdinalFields);
      this.exprBindings = new FieldDefBindings(this.fields);

      this.facetsConfig = new FacetsConfig();
      this.internalFacetFieldNames = new HashSet<>();
      // initialize with previous config
      for (Map.Entry<String, DimConfig> entry : initial.facetsConfig.getDimConfigs().entrySet()) {
        facetsConfig.setHierarchical(entry.getKey(), entry.getValue().hierarchical);
        facetsConfig.setIndexFieldName(entry.getKey(), entry.getValue().indexFieldName);
        facetsConfig.setRequireDimCount(entry.getKey(), entry.getValue().requireDimCount);
        facetsConfig.setRequireDimensionDrillDown(
            entry.getKey(), entry.getValue().requireDimensionDrillDown);
        facetsConfig.setMultiValued(entry.getKey(), entry.getValue().multiValued);
        internalFacetFieldNames.add(entry.getValue().indexFieldName);
      }
    }

    public Bindings getBindings() {
      return exprBindings;
    }

    /**
     * Add {@link FieldDef} into the builder and update any related state.
     *
     * @param fieldDef field def to add
     * @param field corresponding Field message, or null if not a top level field
     * @return builder
     */
    public Builder addField(FieldDef fieldDef, Field field) {
      if (fields.containsKey(fieldDef.getName())) {
        throw new IllegalArgumentException("Field already exists: " + fieldDef.getName());
      }
      if (fieldDef instanceof IdFieldDef) {
        if (idFieldDef != null) {
          throw new IllegalArgumentException(
              "Index can only register one id field, found: "
                  + idFieldDef.getName()
                  + " and "
                  + fieldDef.getName());
        }
        idFieldDef = (IdFieldDef) fieldDef;
      }
      if (fieldDef instanceof TextBaseFieldDef) {
        if (((TextBaseFieldDef) fieldDef).getSearchAnalyzer().isPresent()) {
          indexedAnalyzedFields.add(fieldDef.getName());
        }
      }
      if (fieldDef.getEagerGlobalOrdinals()) {
        eagerGlobalOrdinalFields.put(fieldDef.getName(), fieldDef);
      }
      if (fieldDef instanceof GlobalOrdinalable
          && ((GlobalOrdinalable) fieldDef).getEagerFieldGlobalOrdinals()) {
        eagerFieldGlobalOrdinalFields.put(fieldDef.getName(), (GlobalOrdinalable) fieldDef);
      }
      if (!hasNestedChildFields
          && fieldDef instanceof ObjectFieldDef
          && ((ObjectFieldDef) fieldDef).isNestedDoc()) {
        hasNestedChildFields = true;
      }
      setFacetsConfigForField(fieldDef, field);
      fields.put(fieldDef.getName(), fieldDef);
      return this;
    }

    private void setFacetsConfigForField(FieldDef fieldDef, Field field) {
      // facets not supported for child fields
      if (field != null && fieldDef instanceof IndexableFieldDef) {
        String fieldName = fieldDef.getName();
        IndexableFieldDef indexableFieldDef = (IndexableFieldDef) fieldDef;
        IndexableFieldDef.FacetValueType facetType = indexableFieldDef.getFacetValueType();
        if (facetType != IndexableFieldDef.FacetValueType.NO_FACETS
            && facetType != IndexableFieldDef.FacetValueType.NUMERIC_RANGE) {
          // hierarchy, float or sortedSetDocValues
          if (facetType == IndexableFieldDef.FacetValueType.HIERARCHY) {
            facetsConfig.setHierarchical(fieldName, true);
          }
          if (indexableFieldDef.isMultiValue()) {
            facetsConfig.setMultiValued(fieldName, true);
          }
          // set indexFieldName for HIERARCHY (TAXO), SORTED_SET_DOC_VALUE and FLAT  facet
          String facetFieldName =
              field.getFacetIndexFieldName().isEmpty()
                  ? String.format("$_%s", field.getName())
                  : field.getFacetIndexFieldName();
          facetsConfig.setIndexFieldName(fieldName, facetFieldName);
          internalFacetFieldNames.add(facetFieldName);
        }
      }
    }

    /**
     * Create {@link FieldAndFacetState} from builder.
     *
     * @return field and facet state object
     */
    public FieldAndFacetState build() {
      return new FieldAndFacetState(this);
    }
  }
}
