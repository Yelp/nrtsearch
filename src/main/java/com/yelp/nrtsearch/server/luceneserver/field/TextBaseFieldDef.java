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

import com.yelp.nrtsearch.server.grpc.FacetType;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.luceneserver.Constants;
import com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.luceneserver.doc.DocValuesFactory;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;

/**
 * Base class for all text base field definitions. In addition to the properties from {@link
 * IndexableFieldDef}, text fields have the option for {@link Analyzer}s and highlighting.
 */
public abstract class TextBaseFieldDef extends IndexableFieldDef {

  private final boolean isHighlighted;
  private final Analyzer indexAnalyzer;
  private final Analyzer searchAnalyzer;

  /**
   * Field constructor. Uses {@link IndexableFieldDef#IndexableFieldDef(String, Field)} to do common
   * initialization, then sets up highlighting and analyzers. Analyzers are parsed through calls to
   * the protected methods {@link #parseIndexAnalyzer(Field)} and {@link
   * #parseSearchAnalyzer(Field)}.
   *
   * @param name field name
   * @param requestField field definition from grpc request
   */
  protected TextBaseFieldDef(String name, Field requestField) {
    super(name, requestField);
    isHighlighted = requestField.getHighlight();
    indexAnalyzer = parseIndexAnalyzer(requestField);
    searchAnalyzer = parseSearchAnalyzer(requestField);
  }

  @Override
  protected void validateRequest(Field requestField) {
    super.validateRequest(requestField);

    if (requestField.getSort()) {
      throw new IllegalArgumentException("sort: Cannot sort text fields; use atom instead");
    }

    if (requestField.getHighlight() && !requestField.getSearch()) {
      throw new IllegalArgumentException("search must be true when highlight is true");
    }

    if (requestField.getHighlight() && !requestField.getStore()) {
      throw new IllegalArgumentException("store must be true when highlight is true");
    }
  }

  @Override
  protected DocValuesType parseDocValuesType(Field requestField) {
    if (requestField.getGroup()) {
      return DocValuesType.SORTED;
    } else if (requestField.getStoreDocValues()) {
      // needed to support multivalued text fields even though its not grouped
      // since neither BINARY nor SORTED allows for multiValued fields during indexing
      if (requestField.getMultiValued()) {
        return DocValuesType.SORTED_SET;
      } else {
        return DocValuesType.BINARY;
      }
    } else {
      return DocValuesType.NONE;
    }
  }

  @Override
  protected FacetValueType parseFacetValueType(Field requestField) {
    FacetType facetType = requestField.getFacet();
    if (facetType.equals(FacetType.HIERARCHY)) {
      if (requestField.getHighlight()) {
        throw new IllegalArgumentException("facet=hierarchy fields cannot have highlight=true");
      }
      if (requestField.getSearch()) {
        throw new IllegalArgumentException("facet=hierarchy fields cannot have search=true");
      }
      if (requestField.getStore()) {
        throw new IllegalArgumentException("facet=hierarchy fields cannot have store=true");
      }
      return FacetValueType.HIERARCHY;
    } else if (facetType.equals(FacetType.NUMERIC_RANGE)) {
      throw new IllegalArgumentException("numericRange facets only applies to numeric types");
    } else if (facetType.equals(FacetType.SORTED_SET_DOC_VALUES)) {
      return FacetValueType.SORTED_SET_DOC_VALUES;
    } else if (facetType.equals(FacetType.FLAT)) {
      return FacetValueType.FLAT;
    }
    return FacetValueType.NO_FACETS;
  }

  /**
   * Parse the index time analyzer from the grpc field definition. If analysis is not possible, null
   * may be returned. This method is called by the {@link TextBaseFieldDef} constructor.
   *
   * @param requestField field definition from request
   * @return index time analyzer or null
   */
  protected Analyzer parseIndexAnalyzer(Field requestField) {
    if (requestField.getSearch()) {
      if (AnalyzerCreator.isAnalyzerDefined(requestField.getAnalyzer())) {
        return AnalyzerCreator.getInstance().getAnalyzer(requestField.getAnalyzer());
      } else {
        if (AnalyzerCreator.isAnalyzerDefined(requestField.getIndexAnalyzer())) {
          return AnalyzerCreator.getInstance().getAnalyzer(requestField.getIndexAnalyzer());
        } else {
          return AnalyzerCreator.getStandardAnalyzer();
        }
      }
    } else {
      return null;
    }
  }

  /**
   * Parse the search time analyzer from the grpc field definition. If analysis is not possible,
   * null may be returned. This method is called by the {@link TextBaseFieldDef} constructor.
   *
   * @param requestField field definition from request
   * @return search time analyzer or null
   */
  protected Analyzer parseSearchAnalyzer(Field requestField) {
    if (requestField.getSearch()) {
      if (AnalyzerCreator.isAnalyzerDefined(requestField.getAnalyzer())) {
        return AnalyzerCreator.getInstance().getAnalyzer(requestField.getAnalyzer());
      } else {
        if (AnalyzerCreator.isAnalyzerDefined(requestField.getSearchAnalyzer())) {
          return AnalyzerCreator.getInstance().getAnalyzer(requestField.getSearchAnalyzer());
        } else {
          return AnalyzerCreator.getStandardAnalyzer();
        }
      }
    } else {
      return null;
    }
  }

  @Override
  protected void setSearchProperties(FieldType fieldType, Field requestField) {
    if (requestField.getSearch()) {
      if (requestField.getHighlight()) {
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
      } else {
        switch (requestField.getIndexOptions()) {
          case DOCS:
            fieldType.setIndexOptions(IndexOptions.DOCS);
            break;
          case DOCS_FREQS:
            fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
            break;
          case DOCS_FREQS_POSITIONS:
            fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            break;
          case DOCS_FREQS_POSITIONS_OFFSETS:
            fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
            break;
          default:
            fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        }
      }
    }
    fieldType.setTokenized(requestField.getTokenize());
    fieldType.setOmitNorms(requestField.getOmitNorms());

    switch (requestField.getTermVectors()) {
      case TERMS:
        fieldType.setStoreTermVectors(true);
        break;
      case TERMS_POSITIONS:
        fieldType.setStoreTermVectors(true);
        fieldType.setStoreTermVectorPositions(true);
        break;
      case TERMS_POSITIONS_OFFSETS:
        fieldType.setStoreTermVectors(true);
        fieldType.setStoreTermVectorPositions(true);
        fieldType.setStoreTermVectorOffsets(true);
        break;
      case TERMS_POSITIONS_OFFSETS_PAYLOADS:
        fieldType.setStoreTermVectors(true);
        fieldType.setStoreTermVectorPositions(true);
        fieldType.setStoreTermVectorOffsets(true);
        fieldType.setStoreTermVectorPayloads(true);
        break;
      default:
        break;
    }
  }

  /**
   * Get an optional analyzer to use during indexing. This option may be empty if analysis is not
   * possible for this field. For example, if it is not searchable.
   *
   * @return optional index time analyzer
   */
  public Optional<Analyzer> getIndexAnalyzer() {
    return Optional.ofNullable(indexAnalyzer);
  }

  /**
   * Get an optional analyzer to use during search. This option may be empty if analysis is not
   * possible for this field. For example, if it is not searchable.
   *
   * @return optional search time analyzer
   */
  public Optional<Analyzer> getSearchAnalyzer() {
    return Optional.ofNullable(searchAnalyzer);
  }

  @Override
  public LoadedDocValues<?> getDocValues(LeafReaderContext context) throws IOException {
    if (docValuesType == DocValuesType.BINARY) {
      // The value is stored in a BINARY field, but it is always a String
      BinaryDocValues binaryDocValues = DocValues.getBinary(context.reader(), getName());
      return new LoadedDocValues.SingleString(binaryDocValues);
    } else {
      return DocValuesFactory.getBinaryDocValues(getName(), docValuesType, context);
    }
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    if (fieldValues.size() > 1 && !isMultiValue()) {
      throw new IllegalArgumentException("Cannot index multiple values into single value field");
    }

    for (int i = 0; i < fieldValues.size(); i++) {
      String fieldStr = fieldValues.get(i);
      if (isHighlighted && isMultiValue() && fieldStr.indexOf(Constants.INFORMATION_SEP) != -1) {
        // TODO: we could remove this restriction if it
        // ever matters ... we can highlight multi-valued
        // fields at search time without stealing a
        // character:
        throw new IllegalArgumentException(
            String.format(
                "%s multiValued and highlighted fields cannot contain INFORMATION_SEPARATOR (U+001F) character: this character is used internally when highlighting multi-valued fields",
                getName()));
      }
      if (hasDocValues()) {
        BytesRef stringBytes = new BytesRef(fieldStr);
        if (docValuesType == DocValuesType.BINARY) {
          document.add(new BinaryDocValuesField(getName(), stringBytes));
        } else if (docValuesType == DocValuesType.SORTED) {
          document.add(new SortedDocValuesField(getName(), stringBytes));
        } else if (docValuesType == DocValuesType.SORTED_SET) {
          document.add(new SortedSetDocValuesField(getName(), stringBytes));
        } else {
          throw new IllegalArgumentException("unsupported doc value type: " + docValuesType);
        }
      }

      if (isStored() || isSearchable()) {
        document.add(new FieldWithData(getName(), fieldType, fieldStr));
      }

      addFacet(
          document,
          fieldStr,
          facetHierarchyPaths.isEmpty() ? Collections.emptyList() : facetHierarchyPaths.get(i));
    }
  }

  private void addFacet(Document document, String value, List<String> paths) {
    if (facetValueType == FacetValueType.HIERARCHY) {
      if (paths.isEmpty()) {
        document.add(new FacetField(getName(), value));
      } else {
        document.add(new FacetField(getName(), paths.toArray(new String[paths.size()])));
      }
    } else if (facetValueType == FacetValueType.FLAT) {
      document.add(new FacetField(getName(), value));
    } else if (facetValueType == FacetValueType.SORTED_SET_DOC_VALUES) {
      String facetValue = String.valueOf(value);
      document.add(new SortedSetDocValuesFacetField(getName(), facetValue));
    }
  }

  /**
   * Get if this field is highlighted.
   *
   * @return if this field is highlighted
   */
  public boolean isHighlighted() {
    return isHighlighted;
  }
}
