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
import com.yelp.nrtsearch.server.luceneserver.AddDocumentHandler;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ServerCodec;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.similarity.SimilarityCreator;
import com.yelp.nrtsearch.server.utils.StructValueTransformer;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.similarities.Similarity;

/** Base class for all field definition types that can be written into the index. */
public abstract class IndexableFieldDef extends FieldDef {
  public enum FacetValueType {
    NO_FACETS,
    FLAT,
    HIERARCHY,
    NUMERIC_RANGE,
    SORTED_SET_DOC_VALUES
  }

  private final boolean isStored;
  private final boolean isMultiValue;
  private final boolean isSearchable;
  private final String postingsFormat;
  private final String docValuesFormat;
  private final Similarity similarity;

  private final Map<String, IndexableFieldDef> childFields;

  protected final DocValuesType docValuesType;
  protected final IndexableFieldDef.FacetValueType facetValueType;
  protected final boolean eagerGlobalOrdinals;
  protected final FieldType fieldType;

  /**
   * Field constructor. Performs generalized building of field definition by calling a number of
   * protected methods, mainly: {@link #validateRequest(Field)}, {@link #parseDocValuesType(Field)},
   * {@link #parseFacetValueType(Field)}, and {@link #setSearchProperties(FieldType, Field)}.
   * Concrete field types should override these methods as needed.
   *
   * @param name name of field
   * @param requestField field definition from grpc request
   */
  protected IndexableFieldDef(String name, Field requestField) {
    super(name);

    validateRequest(requestField);
    isStored = requestField.getStore();
    isMultiValue = requestField.getMultiValued();
    isSearchable = requestField.getSearch();
    docValuesType = parseDocValuesType(requestField);

    fieldType = new FieldType();
    fieldType.setStored(requestField.getStore());
    setSearchProperties(fieldType, requestField);
    fieldType.freeze();

    facetValueType = parseFacetValueType(requestField);
    eagerGlobalOrdinals = requestField.getEagerGlobalOrdinals();

    postingsFormat =
        requestField.getPostingsFormat().isEmpty()
            ? ServerCodec.DEFAULT_POSTINGS_FORMAT
            : requestField.getPostingsFormat();
    docValuesFormat =
        requestField.getDocValuesFormat().isEmpty()
            ? ServerCodec.DEFAULT_DOC_VALUES_FORMAT
            : requestField.getDocValuesFormat();

    String similarityStr = requestField.getSimilarity();
    Map<String, Object> similarityParams =
        StructValueTransformer.transformStruct(requestField.getSimilarityParams());
    this.similarity =
        SimilarityCreator.getInstance().createSimilarity(similarityStr, similarityParams);

    // add any children this field has
    if (requestField.getChildFieldsCount() > 0) {
      Map<String, IndexableFieldDef> childFields = new HashMap<>();
      for (Field field : requestField.getChildFieldsList()) {
        checkChildName(field.getName());
        String childName = getName() + IndexState.CHILD_FIELD_SEPARATOR + field.getName();
        FieldDef fieldDef = FieldDefCreator.getInstance().createFieldDef(childName, field);
        if (!(fieldDef instanceof IndexableFieldDef)) {
          throw new IllegalArgumentException("Child field is not indexable: " + childName);
        }
        childFields.put(childName, (IndexableFieldDef) fieldDef);
      }
      this.childFields = Collections.unmodifiableMap(childFields);
    } else {
      this.childFields = Collections.emptyMap();
    }
  }

  private void checkChildName(String name) {
    if (!IndexState.isSimpleName(name)) {
      throw new IllegalArgumentException(
          "invalid child field name \"" + name + "\": must be [a-zA-Z_][a-zA-Z0-9]*");
    }

    if (name.endsWith("_boost")) {
      throw new IllegalArgumentException(
          "invalid child field name \"" + name + "\": field names cannot end with _boost");
    }
  }

  /**
   * Method called by {@link #IndexableFieldDef(String, Field)} to validate the provided {@link
   * Field}. Field definitions should define a version that checks for incompatible parameters and
   * any other potential issues. It is recommended to also call the super version of this method, so
   * that general checks do not need to be repeated everywhere.
   *
   * @param requestField field properties to validate
   */
  protected void validateRequest(Field requestField) {
    if (requestField.getMultiValued() && requestField.getGroup()) {
      throw new IllegalArgumentException(
          String.format(
              "field: %s cannot have both group and multivalued set to true. Cannot  group on multiValued fields",
              requestField.getName()));
    }
  }

  /**
   * Method called by {@link #IndexableFieldDef(String, Field)} to determine the doc value type used
   * by this field. Fields are not necessarily limited to one doc value, but this should represent
   * the primary value that will be accessible to scripts and search through {@link
   * #getDocValues(LeafReaderContext)}. A value of NONE implies that the field does not support doc
   * values.
   *
   * @param requestField field from request
   * @return field doc value type
   */
  protected DocValuesType parseDocValuesType(Field requestField) {
    return DocValuesType.NONE;
  }

  /**
   * Method called by {@link #IndexableFieldDef(String, Field)} to determine the facet value type
   * for this field. The result of this method is exposed externally through {@link
   * #getFacetValueType()}. A value of NO_FACETS implies that the field does not support facets.
   *
   * @param requestField field from request
   * @return field facet value type
   */
  protected FacetValueType parseFacetValueType(Field requestField) {
    return FacetValueType.NO_FACETS;
  }

  /**
   * Method called by {@link #IndexableFieldDef(String, Field)} to set the search properties on the
   * given {@link FieldType}. The {@link FieldType#setStored(boolean)} has already been set to the
   * value from {@link Field#getStore()}. This method should set any other needed properties, such
   * as index options, tokenization, term vectors, etc. It likely should not set a doc value type,
   * as those are usually added separately. The common use of this {@link FieldType} is to add a
   * {@link FieldWithData} during indexing. This method should not freeze the field type.
   *
   * @param fieldType type that needs search properties set
   * @param requestField field from request
   */
  protected void setSearchProperties(FieldType fieldType, Field requestField) {}

  /**
   * Get if this field can have doc values. These values must be accessible via {@link
   * #getDocValues(LeafReaderContext)}.
   *
   * @return if this field has doc values
   */
  public boolean hasDocValues() {
    return docValuesType != DocValuesType.NONE;
  }

  /** Get the type of doc value used for this field. */
  public DocValuesType getDocValuesType() {
    return docValuesType;
  }

  /**
   * Get if this field data is stored in the index. This data must be accessible via {@link
   * #getStored(Document)}.
   *
   * @return if this field is stored in the index
   */
  public boolean isStored() {
    return isStored;
  }

  /**
   * Get if this field can have multiple values.
   *
   * @return if this field can have multiple values
   */
  public boolean isMultiValue() {
    return isMultiValue;
  }

  /**
   * Get if this field is able to be searched.
   *
   * @return if this field can be searched
   */
  public boolean isSearchable() {
    return isSearchable;
  }

  /**
   * Get the facet value type for this field.
   *
   * @return field facet value type
   */
  @Override
  public FacetValueType getFacetValueType() {
    return facetValueType;
  }

  @Override
  public boolean getEagerGlobalOrdinals() {
    return eagerGlobalOrdinals;
  }

  /**
   * Get map of direct children of this field.
   *
   * @return child map
   */
  public Map<String, IndexableFieldDef> getChildFields() {
    return childFields;
  }

  /**
   * Get the doc values for this field, bound to the given lucene segment context. This method
   * should only be called if {@link #isStored()} is true. Contains field values used when
   * retrieving fields during search and when preforming script scoring.
   *
   * @param context lucene segment context
   * @return doc values for field
   * @throws IOException if there is an error loading doc values
   */
  public LoadedDocValues<?> getDocValues(LeafReaderContext context) throws IOException {
    throw new UnsupportedOperationException("Doc values not supported for field: " + getName());
  }

  /**
   * Get the field values stored in the index when the property store=true. Retrieve the String
   * values from the document and perform any needed post processing.
   *
   * @param document lucene document
   * @return String representations of stored field values
   */
  public String[] getStored(Document document) {
    return document.getValues(getName());
  }

  /**
   * Parse a list of field values for this field and its children. The values will be those present
   * in a {@link com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField}.
   *
   * @param documentsContext DocumentsContext which holds lucene documents to be added to the index
   * @param fieldValues list of String encoded field values
   * @param facetHierarchyPaths list of list of String encoded paths for each field value be
   *     determine hierarchy for faceting
   */
  public void parseFieldWithChildren(
      AddDocumentHandler.DocumentsContext documentsContext,
      List<String> fieldValues,
      List<List<String>> facetHierarchyPaths) {
    parseFieldWithChildren(documentsContext.getRootDocument(), fieldValues, facetHierarchyPaths);
  }
  /**
   * Parse a list of field values for this field and its children. The values will be those present
   * in a {@link com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField}.
   *
   * @param document lucene document to be added to the index
   * @param fieldValues list of String encoded field values
   * @param facetHierarchyPaths list of list of String encoded paths for each field value be
   *     determine hierarchy for faceting
   */
  public void parseFieldWithChildren(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    parseDocumentField(document, fieldValues, facetHierarchyPaths);
    childFields.forEach(
        (k, v) -> v.parseFieldWithChildren(document, fieldValues, facetHierarchyPaths));
  }

  /**
   * Parse a list of field values and add them to the document for indexing. The values will be
   * those present in a {@link com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField}.
   *
   * @param document lucene document to be added to the index
   * @param fieldValues list of String encoded field values
   * @param facetHierarchyPaths list of list of String encoded paths for each field value be
   *     determine hierarchy for faceting
   */
  public abstract void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths);

  /**
   * Get Similarity implementation that should be used for this field.
   *
   * @return similarity for this field
   */
  public Similarity getSimilarity() {
    return similarity;
  }

  /**
   * Get the postings format that should be used for this field.
   *
   * @return posting format for this field
   */
  public String getPostingsFormat() {
    return postingsFormat;
  }

  /**
   * Get the doc values format that should be used for this field.
   *
   * @return doc values format for this field
   */
  public String getDocValuesFormat() {
    return docValuesFormat;
  }

  /**
   * Get the Lucene definition for this field.
   *
   * @return {link FieldType} for this field
   */
  public FieldType getFieldType() {
    return fieldType;
  }
}
