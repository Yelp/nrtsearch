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

import com.yelp.nrtsearch.server.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.analysis.PosIncGapAnalyzerWrapper;
import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.field.properties.GlobalOrdinalable;
import com.yelp.nrtsearch.server.field.properties.TermQueryable;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.search.GlobalOrdinalLookup;
import com.yelp.nrtsearch.server.search.GlobalOrdinalLookup.SortedLookup;
import com.yelp.nrtsearch.server.search.GlobalOrdinalLookup.SortedSetLookup;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/**
 * Base class for all text base field definitions. In addition to the properties from {@link
 * IndexableFieldDef}, text fields have the option for {@link Analyzer}s.
 */
public abstract class TextBaseFieldDef extends IndexableFieldDef<String>
    implements TermQueryable, GlobalOrdinalable {
  static final int DEFAULT_POSITION_INCREMENT_GAP = 100;

  private final Analyzer indexAnalyzer;
  private final Analyzer searchAnalyzer;
  private final boolean eagerFieldGlobalOrdinals;

  public final Map<IndexReader.CacheKey, GlobalOrdinalLookup> ordinalLookupCache = new HashMap<>();
  private final Object ordinalBuilderLock = new Object();
  private final int ignoreAbove;

  /**
   * Field constructor. Uses {@link IndexableFieldDef#IndexableFieldDef(String, Field,
   * FieldDefCreator.FieldDefCreatorContext, Class)} to do common initialization, then sets up
   * analyzers. Analyzers are parsed through calls to the protected methods {@link
   * #parseIndexAnalyzer(Field)} and {@link #parseSearchAnalyzer(Field)}.
   *
   * @param name field name
   * @param requestField field definition from grpc request
   * @param context creation context
   */
  protected TextBaseFieldDef(
      String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
    super(name, requestField, context, String.class);
    indexAnalyzer = parseIndexAnalyzer(requestField);
    searchAnalyzer = parseSearchAnalyzer(requestField);
    eagerFieldGlobalOrdinals = requestField.getEagerFieldGlobalOrdinals();
    ignoreAbove = requestField.hasIgnoreAbove() ? requestField.getIgnoreAbove() : Integer.MAX_VALUE;
  }

  @Override
  protected void validateRequest(Field requestField) {
    super.validateRequest(requestField);

    if (requestField.getMultiValued()
        && requestField.getTextDocValuesType() == TextDocValuesType.TEXT_DOC_VALUES_TYPE_BINARY) {
      throw new IllegalArgumentException(
          "Cannot use binary doc values with multiValued fields, invalid field "
              + requestField.getName());
    }

    if (!requestField.getSearch() && requestField.getTermVectors() != TermVectors.NO_TERMVECTORS) {
      throw new IllegalArgumentException(
          "Indexing term vectors requires field to be searchable, invalid field "
              + requestField.getName());
    }
  }

  @Override
  protected DocValuesType parseDocValuesType(Field requestField) {
    if (!requestField.getStoreDocValues()) {
      return DocValuesType.NONE;
    }
    if (requestField.getMultiValued()) {
      // Binary doc values are not supported for multivalued fields
      return DocValuesType.SORTED_SET;
    }
    if (requestField.getTextDocValuesType() == TextDocValuesType.TEXT_DOC_VALUES_TYPE_BINARY) {
      return DocValuesType.BINARY;
    }
    return DocValuesType.SORTED;
  }

  @Override
  protected FacetValueType parseFacetValueType(Field requestField) {
    FacetType facetType = requestField.getFacet();
    if (facetType.equals(FacetType.HIERARCHY)) {
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
      Analyzer analyzer;
      if (AnalyzerCreator.isAnalyzerDefined(requestField.getAnalyzer())) {
        analyzer = AnalyzerCreator.getInstance().getAnalyzer(requestField.getAnalyzer());
      } else {
        if (AnalyzerCreator.isAnalyzerDefined(requestField.getIndexAnalyzer())) {
          analyzer = AnalyzerCreator.getInstance().getAnalyzer(requestField.getIndexAnalyzer());
        } else {
          analyzer = AnalyzerCreator.getStandardAnalyzer();
        }
      }
      int positionIncrementGap =
          requestField.hasPositionIncrementGap()
              ? requestField.getPositionIncrementGap()
              : DEFAULT_POSITION_INCREMENT_GAP;
      return new PosIncGapAnalyzerWrapper(analyzer, positionIncrementGap);
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
  public LoadedDocValues<String> getDocValues(LeafReaderContext context) throws IOException {
    if (docValuesType == DocValuesType.SORTED) {
      SortedDocValues sortedDocValues = DocValues.getSorted(context.reader(), getName());
      return new LoadedDocValues.SingleString(sortedDocValues);
    } else if (docValuesType == DocValuesType.BINARY) {
      BinaryDocValues binaryDocValues = DocValues.getBinary(context.reader(), getName());
      return new LoadedDocValues.SingleBinaryString(binaryDocValues);
    } else if (docValuesType == DocValuesType.SORTED_SET) {
      SortedSetDocValues sortedSetDocValues = DocValues.getSortedSet(context.reader(), getName());
      return new LoadedDocValues.SortedStrings(sortedSetDocValues);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported doc value type %s for field %s", docValuesType, this.getName()));
    }
  }

  @Override
  public SearchResponse.Hit.FieldValue getStoredFieldValue(StoredValue value) {
    return SearchResponse.Hit.FieldValue.newBuilder().setTextValue(value.getStringValue()).build();
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    if (fieldValues.size() > 1 && !isMultiValue()) {
      throw new IllegalArgumentException(
          "Cannot index multiple values into single value field: " + getName());
    }

    for (int i = 0; i < fieldValues.size(); i++) {
      String fieldStr = fieldValues.get(i);
      if (hasDocValues()) {
        BytesRef stringBytes = new BytesRef(fieldStr);
        if (docValuesType == DocValuesType.BINARY) {
          document.add(new BinaryDocValuesField(getName(), stringBytes));
        } else if (fieldStr.length() <= ignoreAbove) {
          if (docValuesType == DocValuesType.SORTED) {
            document.add(new SortedDocValuesField(getName(), stringBytes));
          } else if (docValuesType == DocValuesType.SORTED_SET) {
            document.add(new SortedSetDocValuesField(getName(), stringBytes));
          } else {
            throw new IllegalArgumentException(
                String.format(
                    "Unsupported doc value type %s for field %s", docValuesType, this.getName()));
          }
        }
      }

      if ((isStored() || isSearchable()) && fieldStr.length() <= ignoreAbove) {
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
        document.add(new FacetField(getName(), paths.toArray(new String[0])));
      }
    } else if (facetValueType == FacetValueType.FLAT) {
      document.add(new FacetField(getName(), value));
    } else if (facetValueType == FacetValueType.SORTED_SET_DOC_VALUES) {
      String facetValue = String.valueOf(value);
      document.add(new SortedSetDocValuesFacetField(getName(), facetValue));
    }
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

  @Override
  public boolean usesOrdinals() {
    return docValuesType == DocValuesType.SORTED || docValuesType == DocValuesType.SORTED_SET;
  }

  @Override
  public boolean getEagerFieldGlobalOrdinals() {
    return eagerFieldGlobalOrdinals;
  }

  @Override
  public GlobalOrdinalLookup getOrdinalLookup(IndexReader reader) throws IOException {
    if (!usesOrdinals()) {
      throw new IllegalStateException("Field: " + getName() + " does not use ordinals");
    }

    IndexReader.CacheKey cacheKey = reader.getReaderCacheHelper().getKey();
    GlobalOrdinalLookup ordinalLookup;
    synchronized (ordinalLookupCache) {
      ordinalLookup = ordinalLookupCache.get(cacheKey);
    }

    if (ordinalLookup == null) {
      // use separate lock to build lookup, to not block cache reads
      synchronized (ordinalBuilderLock) {
        // make sure this wasn't built while we waited for the lock
        synchronized (ordinalLookupCache) {
          ordinalLookup = ordinalLookupCache.get(cacheKey);
        }
        if (ordinalLookup == null) {
          // build lookup based on doc value type
          if (docValuesType == DocValuesType.SORTED) {
            ordinalLookup = new SortedLookup(reader, getName());
          } else if (docValuesType == DocValuesType.SORTED_SET) {
            ordinalLookup = new SortedSetLookup(reader, getName());
          } else {
            throw new IllegalStateException(
                "Doc value type not usable for ordinals: " + docValuesType);
          }

          // add lookup to the cache
          synchronized (ordinalLookupCache) {
            ordinalLookupCache.put(cacheKey, ordinalLookup);
            reader
                .getReaderCacheHelper()
                .addClosedListener(
                    key -> {
                      synchronized (ordinalLookupCache) {
                        ordinalLookupCache.remove(key);
                      }
                    });
          }
        }
      }
    }

    return ordinalLookup;
  }

  static void setIndexOptions(
      IndexOptions grpcIndexOptions,
      FieldType fieldType,
      org.apache.lucene.index.IndexOptions defaultOptions) {
    switch (grpcIndexOptions) {
      case DOCS:
        fieldType.setIndexOptions(org.apache.lucene.index.IndexOptions.DOCS);
        break;
      case DOCS_FREQS:
        fieldType.setIndexOptions(org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS);
        break;
      case DOCS_FREQS_POSITIONS:
        fieldType.setIndexOptions(
            org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        break;
      case DOCS_FREQS_POSITIONS_OFFSETS:
        fieldType.setIndexOptions(
            org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        break;
      default:
        fieldType.setIndexOptions(defaultOptions);
        break;
    }
  }
}
