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
package com.yelp.nrtsearch.server.doc;

import com.yelp.nrtsearch.server.field.FieldDef;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.join.BitSetProducer;

/**
 * Index level class for providing access to doc values data. Provides a means to get a {@link
 * SegmentDocLookup} bound to single lucene segment.
 */
public class DocLookup {
  private final Function<String, FieldDef> fieldDefLookup;
  private final Supplier<Collection<String>> allFieldNamesSupplier;
  private final SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy;
  private final Map<String, BitSetProducer> allLevelBitSetProducers;
  private final Function<String, BitSetProducer> childPathFilterLookup;

  /**
   * Constructor.
   *
   * @param fieldDefLookup lookup to produce a field definition from its name
   * @param allFieldNamesSupplier supplier to produce a collection of all valid field names
   * @param searcherAndTaxonomy searcher and taxonomy for the index
   */
  public DocLookup(
      Function<String, FieldDef> fieldDefLookup,
      Supplier<Collection<String>> allFieldNamesSupplier,
      SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy) {
    this(fieldDefLookup, allFieldNamesSupplier, searcherAndTaxonomy, null, null);
  }

  /**
   * Full constructor with nested document support.
   *
   * @param fieldDefLookup lookup to produce a field definition from its name
   * @param allFieldNamesSupplier supplier to produce a collection of all valid field names
   * @param searcherAndTaxonomy searcher and taxonomy for the index
   * @param allLevelBitSetProducers map from nested path name (including "_root") to BitSetProducer
   *     for that level; used by _CHILDREN. to resolve the parent boundary from the field's schema
   *     position; null if the index has no nested documents
   * @param childPathFilterLookup resolves a child field name to a BitSetProducer for its nested
   *     path, enabling filtering when multiple nested paths exist; null if not needed
   */
  public DocLookup(
      Function<String, FieldDef> fieldDefLookup,
      Supplier<Collection<String>> allFieldNamesSupplier,
      SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy,
      Map<String, BitSetProducer> allLevelBitSetProducers,
      Function<String, BitSetProducer> childPathFilterLookup) {
    this.fieldDefLookup = fieldDefLookup;
    this.allFieldNamesSupplier = allFieldNamesSupplier;
    this.searcherAndTaxonomy = searcherAndTaxonomy;
    this.allLevelBitSetProducers = allLevelBitSetProducers;
    this.childPathFilterLookup = childPathFilterLookup;
  }

  /**
   * Get the doc value lookup accessor bound to the given lucene segment. Passes
   * allLevelBitSetProducers and childPathFilterLookup so that SegmentDocLookup can navigate to and
   * filter child documents.
   */
  public SegmentDocLookup getSegmentLookup(LeafReaderContext context) {
    return new SegmentDocLookup(
        fieldDefLookup, context, allLevelBitSetProducers, childPathFilterLookup);
  }

  /**
   * Get the field definition for the given field name.
   *
   * @param fieldName field name
   * @return field definition
   */
  public FieldDef getFieldDef(String fieldName) {
    return fieldDefLookup.apply(fieldName);
  }

  /**
   * Get the field definition for the given field name, or throw an exception if the field does not
   * exist.
   *
   * @param fieldName field name
   * @return field definition
   * @throws IllegalArgumentException if the field does not exist
   */
  public FieldDef getFieldDefOrThrow(String fieldName) {
    FieldDef fieldDef = getFieldDef(fieldName);
    if (fieldDef == null) {
      throw new IllegalArgumentException("field \"" + fieldName + "\" is unknown");
    }
    return fieldDef;
  }

  /** Get a collection of all the existing field names. */
  public Collection<String> getAllFieldNames() {
    return allFieldNamesSupplier.get();
  }

  /**
   * Get the searcher for this request, or null if unavailable. Currently, this is only available
   * for query supplied virtual or runtime fields.
   */
  public SearcherTaxonomyManager.SearcherAndTaxonomy getSearcherAndTaxonomy() {
    return searcherAndTaxonomy;
  }
}
