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
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.properties.TermQueryable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/** Field class for defining '_ID' fields which are used to update documents */
public class IdFieldDef extends IndexableFieldDef implements TermQueryable {

  protected IdFieldDef(String name, Field requestField) {
    super(name, requestField);
  }

  /**
   * We use _ID fields as strings to store and update documents. Hence we fail if the field requests
   * for properties that are not applicable for this use
   *
   * @param requestField field properties to validate
   */
  protected void validateRequest(Field requestField) {
    if (requestField.getMultiValued() || requestField.getTokenize()) {
      throw new IllegalArgumentException(
          String.format(
              "field: %s cannot have multivalued fields or tokenization as it's an _ID field",
              requestField.getName()));
    }
    if (!requestField.getStore() && !requestField.getStoreDocValues()) {
      throw new IllegalArgumentException(
          String.format(
              "field: %s is an _ID field and should be retrievable by either store=true or storeDocValues=true",
              requestField.getName()));
    }
  }

  /**
   * _ID fields should always have indexing turned on so that they can be retrieved for updates
   * Also, these fields cannot be tokenized since we would like to use the values as it is
   *
   * @param fieldType type that needs search properties set
   * @param requestField field from request
   */
  protected void setSearchProperties(FieldType fieldType, Field requestField) {
    fieldType.setIndexOptions(IndexOptions.DOCS);
    fieldType.setOmitNorms(true);
    fieldType.setTokenized(false);
  }

  /**
   * Store the docvalues if it's requested and store the string value in the document
   *
   * @param document lucene document to be added to the index
   * @param fieldValues list of String encoded field values
   * @param facetHierarchyPaths list of list of String encoded paths for each field value be
   */
  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    if (fieldValues.size() > 1) {
      throw new IllegalArgumentException(
          "Cannot index multiple values into _id fields, field name: " + getName());
    }
    String fieldStr = fieldValues.get(0);
    if (hasDocValues()) {
      BytesRef stringBytes = new BytesRef(fieldStr);
      document.add(new BinaryDocValuesField(getName(), stringBytes));
    }
    document.add(new FieldWithData(getName(), fieldType, fieldStr));
  }

  @Override
  public DocValuesType parseDocValuesType(Field requestField) {
    if (requestField.getStoreDocValues()) {
      return DocValuesType.BINARY;
    }
    return DocValuesType.NONE;
  }

  /**
   * Store the doc values in binary format if requested
   *
   * @param context lucene segment context
   * @return
   * @throws IOException
   */
  @Override
  public LoadedDocValues<?> getDocValues(LeafReaderContext context) throws IOException {
    if (hasDocValues()) {
      // The value is stored in a BINARY field, but it is always a String
      BinaryDocValues binaryDocValues = DocValues.getBinary(context.reader(), getName());
      return new LoadedDocValues.SingleString(binaryDocValues);
    }
    throw new IllegalStateException(
        String.format("Unsupported doc value type %s for field %s", docValuesType, this.getName()));
  }

  @Override
  public String getType() {
    return "_ID";
  }

  /**
   * Construct a Term with the given field and value to identify the document to be added or updated
   *
   * @param document the document to be added or updated
   * @return a Term with field and value
   */
  public Term getTerm(Document document) {
    String fieldName = this.getName();
    if (fieldName == null) {
      throw new IllegalArgumentException(
          "The _ID field should have a name to be able to build a Term for updating the document");
    }
    String fieldValue = document.get(fieldName);
    if (fieldValue == null) {
      throw new IllegalArgumentException(
          "Document cannot have a null field value for _ID field: " + fieldName);
    }
    return new Term(fieldName, fieldValue);
  }

  @Override
  public Query getTermQueryFromTextValue(String textValue) {
    return new org.apache.lucene.search.TermQuery(new Term(getName(), textValue));
  }

  @Override
  public Query getTermInSetQueryFromTextValues(List<String> textValues) {
    List<BytesRef> textTerms = textValues.stream().map(BytesRef::new).collect(Collectors.toList());
    return new org.apache.lucene.search.TermInSetQuery(getName(), textTerms);
  }
}
