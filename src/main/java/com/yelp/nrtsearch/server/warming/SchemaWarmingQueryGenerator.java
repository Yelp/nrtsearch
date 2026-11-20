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
package com.yelp.nrtsearch.server.warming;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.type.LatLng;
import com.yelp.nrtsearch.server.field.AtomFieldDef;
import com.yelp.nrtsearch.server.field.BooleanFieldDef;
import com.yelp.nrtsearch.server.field.DateTimeFieldDef;
import com.yelp.nrtsearch.server.field.DoubleFieldDef;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.FloatFieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef.FacetValueType;
import com.yelp.nrtsearch.server.field.IntFieldDef;
import com.yelp.nrtsearch.server.field.LatLonFieldDef;
import com.yelp.nrtsearch.server.field.LongFieldDef;
import com.yelp.nrtsearch.server.field.NumberFieldDef;
import com.yelp.nrtsearch.server.field.ObjectFieldDef;
import com.yelp.nrtsearch.server.field.PolygonfieldDef;
import com.yelp.nrtsearch.server.field.TextFieldDef;
import com.yelp.nrtsearch.server.field.VectorFieldDef;
import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.BooleanQuery;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.ExistsQuery;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.GeoBoundingBoxQuery;
import com.yelp.nrtsearch.server.grpc.GeoPointQuery;
import com.yelp.nrtsearch.server.grpc.GeoRadiusQuery;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.MatchOperator;
import com.yelp.nrtsearch.server.grpc.MatchPhraseQuery;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.MultiMatchQuery;
import com.yelp.nrtsearch.server.grpc.NestedQuery;
import com.yelp.nrtsearch.server.grpc.Point;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.grpc.TopHitsCollector;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.script.ScriptService;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates warming queries from the index schema, so a replica can warm without downloading a
 * query corpus from S3. Values are sampled from the index itself (most frequent terms, point value
 * ranges, geo points, stored vectors), so the queries match real documents and load the index
 * blocks live traffic uses. Text values are read from randomly chosen documents, so common values
 * are picked as often as they occur. Falls back to synthetic values only when a field has nothing
 * to sample. Queries vary topHits and retrieve a rotating subset of doc values fields, so the fetch
 * phase is warmed as well, and no request is generated twice.
 *
 * <p>Queries are generated in stages:
 *
 * <ol>
 *   <li>Filters: single field (60%), two field (30%) and three field (10%) FILTER queries of term,
 *       term set and range leaves. Fields of nested documents are queried through a nested query.
 *   <li>Structured, half of the queries when the index has fields for them. These are the query
 *       shapes live traffic adds on top of filters, each warming its own index structures and code
 *       paths:
 *       <ul>
 *         <li>Boolean: MUST, FILTER, SHOULD with minimumNumberShouldMatch, MUST_NOT and exists
 *             clauses, an inner boolean query, and several clauses inside one nested query.
 *         <li>Sort: by one or two doc values fields, with up to {@link #LARGE_TOP_HITS} hits.
 *         <li>Text: match, match phrase and multi match queries with terms sampled from TEXT
 *             fields.
 *         <li>Geo: radius and bounding box queries around points sampled from LAT_LON fields, point
 *             in polygon queries on POLYGON fields, and distance sort.
 *         <li>Script: virtual fields over numeric doc values, painless when the painless plugin is
 *             installed and js otherwise, sometimes used as the sort.
 *         <li>Aggregation: terms collectors, some with a nested top hits collector.
 *       </ul>
 *   <li>Facets: facet-only requests over every faceted field, in groups.
 *   <li>Vectors: kNN queries per vector field, seeded with vectors read from the index.
 * </ol>
 *
 * <p>The requests are shuffled, so a warming duration limit shortens every stage alike.
 *
 * <p>Generated scripts warm the script engine, doc values access from scripts and script sorting.
 * They do not warm the compilation of the scripts live queries use, which are not part of the
 * schema. No rescorers or plugin queries are generated, so model inference is not warmed.
 */
public class SchemaWarmingQueryGenerator {
  private static final Logger logger = LoggerFactory.getLogger(SchemaWarmingQueryGenerator.class);

  private static final String EPOCH_MILLIS = "epoch_millis";
  private static final String STRICT_DATE_OPTIONAL_TIME = "strict_date_optional_time";
  // topHits of generated filter queries, cycled so collection of small and larger result sets is
  // warmed
  static final int[] TOP_HITS = {10, 30, 100};
  // topHits of structured queries, live queries often fetch a few hundred hits for rescoring
  static final int[] LARGE_TOP_HITS = {10, 100, 250, 500};
  // maximum distinct text values sampled per field, the pool term and term set queries draw from
  static final int MAX_SAMPLED_TERMS = 100;
  // random documents read per wanted value, bounds sampling time on sparse fields
  static final int SAMPLE_DOCS_PER_TERM = 5;
  // term set queries use between 2 and this many sampled values
  static final int MAX_TERM_SET_SIZE = 30;
  // generate up to this many times the wanted queries, skipping duplicates
  static final int MAX_ATTEMPTS_FACTOR = 3;
  // Stop scanning a field's terms after this many, so sampling stays fast on huge vocabularies
  static final int MAX_SCANNED_TERMS = 10_000;
  // random prefixes looked up in a TEXT field's terms, so terms are sampled across the vocabulary
  static final int TEXT_TERM_PROBES = 500;
  static final int MIN_TEXT_TERM_PROBES_PER_SEGMENT = 20;
  static final int FACET_FIELDS_PER_REQUEST = 5;
  static final int FACET_TOP_N = 10;
  static final int NUM_VECTOR_SEEDS = 100;
  static final int KNN_QUERIES_PER_FIELD = 20;
  static final int KNN_K = 100;
  static final int KNN_NUM_CANDIDATES = 200;
  static final int NUM_GEO_POINTS = 100;
  static final String[] GEO_RADII = {"500 m", "2 km", "10 km", "25 km", "50 mi"};
  static final double[] GEO_BOX_HALF_SIZES = {0.01, 0.05, 0.2, 0.5};
  // distinct sources per script language, each is compiled once
  static final int MAX_SCRIPT_SOURCES = 20;
  static final int MAX_SCRIPT_FIELDS = 3;
  static final int[] TERMS_COLLECTOR_SIZES = {10, 50, 100};
  static final int NESTED_TOP_HITS = 3;
  static final double SINGLE_FIELD_FRACTION = 0.6;
  static final double TWO_FIELD_FRACTION = 0.3;
  // share of the queries that are structured, when the index supports any structured kind
  static final double STRUCTURED_FRACTION = 0.5;
  static final String VIRTUAL_FIELD_PREFIX = "warming_vf_";
  static final String COLLECTOR_PREFIX = "warming_terms_";
  static final String NESTED_COLLECTOR_NAME = "warming_top_hits";
  static final String PAINLESS_LANG = "painless";
  static final String JS_LANG = "js";
  private static final String PREFIX_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";
  private static final double[] SCRIPT_WEIGHTS = {0.5, 1.0, 2.0};
  private static final Pattern JS_VARIABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
  private static final NestedQuery.ScoreMode[] NESTED_SCORE_MODES = {
    NestedQuery.ScoreMode.NONE, NestedQuery.ScoreMode.MAX, NestedQuery.ScoreMode.SUM
  };

  /** Kinds of structured queries, weighted by their share of the structured queries. */
  enum StructuredKind {
    BOOLEAN(3),
    SORT(2),
    TEXT(2),
    GEO(2),
    SCRIPT(1),
    AGGREGATION(1);

    final int weight;

    StructuredKind(int weight) {
      this.weight = weight;
    }
  }

  private final IndexState indexState;
  private final Random random;
  // generated topHits are capped to this, 0 for no limit
  private final int maxTopHits;

  /**
   * A searchable field, with values sampled from the index to build queries from.
   *
   * @param nestedPath path of the nested document the field is part of, null for the root document
   */
  @VisibleForTesting
  record FieldSample(
      IndexableFieldDef<?> fieldDef,
      List<String> terms,
      double min,
      double max,
      String nestedPath) {
    FieldSample(IndexableFieldDef<?> fieldDef, List<String> terms, double min, double max) {
      this(fieldDef, terms, min, max, null);
    }

    FieldSample withNestedPath(String path) {
      return new FieldSample(fieldDef, terms, min, max, path);
    }

    boolean hasRange() {
      return min <= max;
    }
  }

  /** A LAT_LON field, with points sampled from its documents as latitude, longitude pairs. */
  @VisibleForTesting
  record GeoSample(LatLonFieldDef fieldDef, List<double[]> points) {}

  /** Fields of the index, grouped by the queries they are used in. */
  private static class SchemaFields {
    final List<FieldSample> fieldSamples = new ArrayList<>();
    final List<FieldSample> textSamples = new ArrayList<>();
    final List<LatLonFieldDef> geoFields = new ArrayList<>();
    final List<GeoSample> geoSamples = new ArrayList<>();
    final List<PolygonfieldDef> polygonFields = new ArrayList<>();
    final List<String> sortFields = new ArrayList<>();
    final List<String> scriptFields = new ArrayList<>();
    final List<String> aggregationFields = new ArrayList<>();
    final List<String> existsFields = new ArrayList<>();
    final List<IndexableFieldDef<?>> facetFields = new ArrayList<>();
    final List<VectorFieldDef<?>> vectorFields = new ArrayList<>();
    final List<String> docValuesFields = new ArrayList<>();
  }

  /** Values sampled from the index, that structured queries are built from. */
  private record Samples(
      List<FieldSample> usable,
      Map<String, List<FieldSample>> usableByNestedPath,
      List<FieldSample> textSamples,
      List<GeoSample> geoSamples,
      List<double[]> geoPoints,
      SchemaFields fields,
      Map<String, List<String>> scriptSources) {}

  public SchemaWarmingQueryGenerator(IndexState indexState) {
    this(indexState, 0);
  }

  /**
   * @param indexState index to generate queries for
   * @param maxTopHits cap generated topHits to this, 0 for no limit
   */
  public SchemaWarmingQueryGenerator(IndexState indexState, int maxTopHits) {
    this(indexState, new Random(), maxTopHits);
  }

  @VisibleForTesting
  SchemaWarmingQueryGenerator(IndexState indexState, Random random) {
    this(indexState, random, 0);
  }

  @VisibleForTesting
  SchemaWarmingQueryGenerator(IndexState indexState, Random random, int maxTopHits) {
    this.indexState = indexState;
    this.random = random;
    this.maxTopHits = maxTopHits;
  }

  /**
   * Generate warming queries for the index.
   *
   * @param numQueries number of filter and structured queries to generate, facet and vector queries
   *     are added on top of this
   * @return generated queries, empty if the index has no searchable fields
   */
  public List<SearchRequest> generate(int numQueries) throws IOException {
    SchemaFields fields = collectFields();

    ShardState shardState = indexState.getShard(0);
    SearcherAndTaxonomy searcherAndTaxonomy = shardState.acquire();
    List<VectorSeeds> vectorSeeds = new ArrayList<>();
    try {
      IndexSearcher searcher = searcherAndTaxonomy.searcher();
      for (int i = 0; i < fields.fieldSamples.size(); ++i) {
        FieldSample sample = fields.fieldSamples.get(i);
        try {
          fields.fieldSamples.set(
              i,
              sampleField(searcher, sample.fieldDef(), random).withNestedPath(sample.nestedPath()));
        } catch (IOException | RuntimeException e) {
          // one field with unexpected index structures should not stop warming the others
          logSamplingFailure(sample.fieldDef(), e);
        }
      }
      for (int i = 0; i < fields.textSamples.size(); ++i) {
        IndexableFieldDef<?> fieldDef = fields.textSamples.get(i).fieldDef();
        try {
          fields.textSamples.set(
              i, new FieldSample(fieldDef, sampleTextTerms(searcher, fieldDef, random), 1, 0));
        } catch (IOException | RuntimeException e) {
          logSamplingFailure(fieldDef, e);
        }
      }
      for (LatLonFieldDef geoField : fields.geoFields) {
        try {
          fields.geoSamples.add(
              new GeoSample(geoField, sampleGeoPoints(searcher, geoField, random)));
        } catch (IOException | RuntimeException e) {
          logSamplingFailure(geoField, e);
        }
      }
      for (VectorFieldDef<?> vectorField : fields.vectorFields) {
        vectorSeeds.add(sampleVectors(searcher, vectorField));
      }
    } finally {
      shardState.release(searcherAndTaxonomy);
    }

    Samples samples = buildSamples(fields);
    List<StructuredKind> kinds = applicableKinds(samples);
    int numStructured = kinds.isEmpty() ? 0 : (int) (numQueries * STRUCTURED_FRACTION);

    List<SearchRequest> requests = new ArrayList<>();
    addFilterQueries(
        requests, samples.usable(), fields.docValuesFields, numQueries - numStructured);
    Map<StructuredKind, Integer> structuredCounts =
        addStructuredQueries(requests, samples, kinds, numStructured);
    addFacetQueries(requests, fields.facetFields);
    for (VectorSeeds seeds : vectorSeeds) {
      addKnnQueries(requests, seeds);
    }
    // a duration limit then cuts every stage short alike, instead of skipping the last stages
    Collections.shuffle(requests, random);
    logger.info(
        "Generated {} schema warming queries for index: {} from {} searchable ({} nested), {} text,"
            + " {} geo, {} polygon, {} sort, {} script, {} aggregation, {} facet and {} vector"
            + " fields, structured queries: {}",
        requests.size(),
        indexState.getName(),
        fields.fieldSamples.size(),
        fields.fieldSamples.stream().filter(s -> s.nestedPath() != null).count(),
        fields.textSamples.size(),
        fields.geoFields.size(),
        fields.polygonFields.size(),
        fields.sortFields.size(),
        fields.scriptFields.size(),
        fields.aggregationFields.size(),
        fields.facetFields.size(),
        fields.vectorFields.size(),
        structuredCounts);
    return requests;
  }

  private void logSamplingFailure(FieldDef fieldDef, Exception e) {
    logger.warn(
        "Unable to sample field: {} of index: {} for schema warming, skipping it",
        fieldDef.getName(),
        indexState.getName(),
        e);
  }

  private SchemaFields collectFields() {
    SchemaFields fields = new SchemaFields();
    for (FieldDef fieldDef : indexState.getAllFields().values()) {
      if (!(fieldDef instanceof IndexableFieldDef<?> indexableFieldDef)
          || fieldDef instanceof ObjectFieldDef) {
        continue;
      }
      String name = fieldDef.getName();
      String nestedPath;
      try {
        nestedPath = nestedPath(name);
      } catch (RuntimeException e) {
        logger.warn("Unable to resolve nested path of field: {}, skipping it", name, e);
        continue;
      }
      boolean inRootDoc = nestedPath == null;
      // child fields (name.child) of the root document can be queried, but are not retrieved,
      // faceted or used in scripts by name
      boolean topLevel = inRootDoc && !name.contains(".");
      if (indexableFieldDef instanceof VectorFieldDef<?> vectorFieldDef) {
        if (topLevel && vectorFieldDef.isSearchable()) {
          fields.vectorFields.add(vectorFieldDef);
        }
        continue;
      }
      if (topLevel && indexableFieldDef.hasDocValues()) {
        fields.docValuesFields.add(name);
      }
      FacetValueType facetValueType = indexableFieldDef.getFacetValueType();
      if (topLevel
          && facetValueType != FacetValueType.NO_FACETS
          && facetValueType != FacetValueType.NUMERIC_RANGE) {
        fields.facetFields.add(indexableFieldDef);
      }
      if (indexableFieldDef.isSearchable() && isSupported(indexableFieldDef)) {
        fields.fieldSamples.add(new FieldSample(indexableFieldDef, List.of(), 1, 0, nestedPath));
      }
      if (!inRootDoc) {
        continue;
      }
      if (indexableFieldDef instanceof TextFieldDef && indexableFieldDef.isSearchable()) {
        fields.textSamples.add(new FieldSample(indexableFieldDef, List.of(), 1, 0));
      }
      if (indexableFieldDef instanceof LatLonFieldDef latLonFieldDef
          && (latLonFieldDef.isSearchable() || latLonFieldDef.hasDocValues())) {
        fields.geoFields.add(latLonFieldDef);
      }
      if (indexableFieldDef instanceof PolygonfieldDef polygonFieldDef
          && polygonFieldDef.isSearchable()) {
        fields.polygonFields.add(polygonFieldDef);
      }
      if (isSortable(indexableFieldDef)) {
        fields.sortFields.add(name);
      }
      if (topLevel
          && indexableFieldDef instanceof NumberFieldDef<?>
          && indexableFieldDef.hasDocValues()
          && !indexableFieldDef.isMultiValue()) {
        fields.scriptFields.add(name);
      }
      if (indexableFieldDef.hasDocValues() && isAggregatable(indexableFieldDef)) {
        fields.aggregationFields.add(name);
      }
      if (indexableFieldDef.isSearchable() || indexableFieldDef.hasDocValues()) {
        fields.existsFields.add(name);
      }
    }
    return fields;
  }

  /** Path of the nested document a field is part of, or null for the root document. */
  private String nestedPath(String fieldName) {
    if (!fieldName.contains(".")) {
      return null;
    }
    String path = IndexState.getFieldBaseNestedPath(fieldName, indexState);
    return path == null || path.equals(IndexState.ROOT) ? null : path;
  }

  private static boolean isSupported(IndexableFieldDef<?> fieldDef) {
    return fieldDef instanceof AtomFieldDef
        || fieldDef instanceof BooleanFieldDef
        || fieldDef instanceof IntFieldDef
        || fieldDef instanceof LongFieldDef
        || fieldDef instanceof FloatFieldDef
        || fieldDef instanceof DoubleFieldDef
        || fieldDef instanceof DateTimeFieldDef;
  }

  private static boolean isSortable(IndexableFieldDef<?> fieldDef) {
    if (!fieldDef.hasDocValues()) {
      return false;
    }
    if (fieldDef instanceof AtomFieldDef) {
      // binary doc values cannot be sorted on
      return fieldDef.getDocValuesType() == DocValuesType.SORTED
          || fieldDef.getDocValuesType() == DocValuesType.SORTED_SET;
    }
    return fieldDef instanceof NumberFieldDef<?> || fieldDef instanceof DateTimeFieldDef;
  }

  private static boolean isAggregatable(IndexableFieldDef<?> fieldDef) {
    return fieldDef instanceof AtomFieldDef
        || fieldDef instanceof IntFieldDef
        || fieldDef instanceof LongFieldDef
        || fieldDef instanceof FloatFieldDef
        || fieldDef instanceof DoubleFieldDef;
  }

  /**
   * Sample values of a text field from random documents, or read the value range of a point field.
   * Text fields without doc values fall back to the most frequent of their first terms.
   */
  @VisibleForTesting
  static FieldSample sampleField(
      IndexSearcher searcher, IndexableFieldDef<?> fieldDef, Random random) throws IOException {
    String field = fieldDef.getName();
    if (fieldDef instanceof AtomFieldDef && fieldDef.hasDocValues()) {
      List<String> values = sampleDocValues(searcher, field, random);
      if (!values.isEmpty()) {
        return new FieldSample(fieldDef, values, 1, 0);
      }
    }
    if (fieldDef instanceof AtomFieldDef) {
      PriorityQueue<TermAndFreq> topTerms =
          new PriorityQueue<>(Comparator.comparingInt(TermAndFreq::docFreq));
      for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
        Terms terms = leaf.reader().terms(field);
        if (terms == null) {
          continue;
        }
        TermsEnum termsEnum = terms.iterator();
        BytesRef term;
        int scanned = 0;
        while ((term = termsEnum.next()) != null && scanned++ < MAX_SCANNED_TERMS) {
          topTerms.add(new TermAndFreq(term.utf8ToString(), termsEnum.docFreq()));
          if (topTerms.size() > MAX_SAMPLED_TERMS) {
            topTerms.poll();
          }
        }
      }
      List<String> values = new ArrayList<>(topTerms.size());
      topTerms.stream().map(TermAndFreq::term).distinct().forEach(values::add);
      return new FieldSample(fieldDef, values, 1, 0);
    }
    if (fieldDef instanceof BooleanFieldDef) {
      return new FieldSample(fieldDef, List.of("true", "false"), 1, 0);
    }
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
      PointValues pointValues = leaf.reader().getPointValues(field);
      if (pointValues == null) {
        continue;
      }
      min = Math.min(min, decodePoint(fieldDef, pointValues.getMinPackedValue()));
      max = Math.max(max, decodePoint(fieldDef, pointValues.getMaxPackedValue()));
    }
    return new FieldSample(fieldDef, List.of(), min, max);
  }

  /**
   * Read the values of randomly chosen live documents. A value that occurs in many documents is
   * picked as often as live queries are likely to use it, unlike the first terms in sort order.
   */
  @VisibleForTesting
  static List<String> sampleDocValues(IndexSearcher searcher, String field, Random random)
      throws IOException {
    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    int maxDoc = searcher.getIndexReader().maxDoc();
    Set<String> values = new LinkedHashSet<>();
    if (maxDoc == 0) {
      return List.of();
    }
    for (int attempt = 0;
        attempt < MAX_SAMPLED_TERMS * SAMPLE_DOCS_PER_TERM && values.size() < MAX_SAMPLED_TERMS;
        ++attempt) {
      int doc = random.nextInt(maxDoc);
      LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(doc, leaves));
      int leafDoc = doc - leaf.docBase;
      Bits liveDocs = leaf.reader().getLiveDocs();
      if (liveDocs != null && !liveDocs.get(leafDoc)) {
        continue;
      }
      FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(field);
      DocValuesType docValuesType =
          fieldInfo == null ? DocValuesType.NONE : fieldInfo.getDocValuesType();
      // doc values iterators only move forward, so use a new one for each random document
      if (docValuesType == DocValuesType.SORTED || docValuesType == DocValuesType.SORTED_SET) {
        SortedSetDocValues docValues = DocValues.getSortedSet(leaf.reader(), field);
        if (!docValues.advanceExact(leafDoc)) {
          continue;
        }
        int pick = random.nextInt(docValues.docValueCount());
        long ord = docValues.nextOrd();
        for (int i = 0; i < pick; ++i) {
          ord = docValues.nextOrd();
        }
        values.add(docValues.lookupOrd(ord).utf8ToString());
      } else if (docValuesType == DocValuesType.BINARY) {
        // single valued text fields may store their value as binary doc values
        BinaryDocValues docValues = leaf.reader().getBinaryDocValues(field);
        if (docValues != null && docValues.advanceExact(leafDoc)) {
          values.add(docValues.binaryValue().utf8ToString());
        }
      } else if (docValuesType != DocValuesType.NONE) {
        // other doc values types do not hold the indexed text, use the term scan instead
        return List.of();
      }
    }
    return new ArrayList<>(values);
  }

  /**
   * Sample analyzed terms of a TEXT field. Terms are looked up at random prefixes, so they come
   * from the whole vocabulary rather than its start in sort order, and the most frequent are kept.
   */
  @VisibleForTesting
  static List<String> sampleTextTerms(
      IndexSearcher searcher, IndexableFieldDef<?> fieldDef, Random random) throws IOException {
    String field = fieldDef.getName();
    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    if (leaves.isEmpty()) {
      return List.of();
    }
    int probesPerLeaf =
        Math.max(MIN_TEXT_TERM_PROBES_PER_SEGMENT, TEXT_TERM_PROBES / leaves.size());
    Map<String, Integer> docFreqs = new HashMap<>();
    for (LeafReaderContext leaf : leaves) {
      Terms terms = leaf.reader().terms(field);
      if (terms == null) {
        continue;
      }
      TermsEnum termsEnum = terms.iterator();
      for (int probe = 0; probe < probesPerLeaf; ++probe) {
        int prefixLength = 1 + random.nextInt(3);
        StringBuilder prefix = new StringBuilder(prefixLength);
        for (int i = 0; i < prefixLength; ++i) {
          prefix.append(PREFIX_CHARS.charAt(random.nextInt(PREFIX_CHARS.length())));
        }
        if (termsEnum.seekCeil(new BytesRef(prefix)) == TermsEnum.SeekStatus.END) {
          continue;
        }
        docFreqs.merge(termsEnum.term().utf8ToString(), termsEnum.docFreq(), Integer::sum);
      }
    }
    return docFreqs.entrySet().stream()
        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
        .limit(MAX_SAMPLED_TERMS)
        .map(Map.Entry::getKey)
        .toList();
  }

  /**
   * Sample points of a LAT_LON field from random documents' doc values. Without doc values, random
   * points inside the field's bounding box are used.
   */
  @VisibleForTesting
  static List<double[]> sampleGeoPoints(
      IndexSearcher searcher, LatLonFieldDef fieldDef, Random random) throws IOException {
    String field = fieldDef.getName();
    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    int maxDoc = searcher.getIndexReader().maxDoc();
    List<double[]> points = new ArrayList<>();
    if (maxDoc == 0) {
      return points;
    }
    if (fieldDef.hasDocValues()) {
      for (int attempt = 0;
          attempt < NUM_GEO_POINTS * SAMPLE_DOCS_PER_TERM && points.size() < NUM_GEO_POINTS;
          ++attempt) {
        int doc = random.nextInt(maxDoc);
        LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(doc, leaves));
        int leafDoc = doc - leaf.docBase;
        Bits liveDocs = leaf.reader().getLiveDocs();
        if (liveDocs != null && !liveDocs.get(leafDoc)) {
          continue;
        }
        SortedNumericDocValues docValues = DocValues.getSortedNumeric(leaf.reader(), field);
        if (docValues.advanceExact(leafDoc)) {
          // LatLonDocValuesField packs the encoded latitude in the high and longitude in the low
          // 32 bits
          long encoded = docValues.nextValue();
          points.add(
              new double[] {
                GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32)),
                GeoEncodingUtils.decodeLongitude((int) encoded)
              });
        }
      }
    }
    if (points.isEmpty() && fieldDef.isSearchable()) {
      double minLat = Double.POSITIVE_INFINITY;
      double maxLat = Double.NEGATIVE_INFINITY;
      double minLon = Double.POSITIVE_INFINITY;
      double maxLon = Double.NEGATIVE_INFINITY;
      for (LeafReaderContext leaf : leaves) {
        PointValues pointValues = leaf.reader().getPointValues(field);
        if (pointValues == null) {
          continue;
        }
        byte[] minPacked = pointValues.getMinPackedValue();
        byte[] maxPacked = pointValues.getMaxPackedValue();
        minLat = Math.min(minLat, GeoEncodingUtils.decodeLatitude(minPacked, 0));
        maxLat = Math.max(maxLat, GeoEncodingUtils.decodeLatitude(maxPacked, 0));
        minLon = Math.min(minLon, GeoEncodingUtils.decodeLongitude(minPacked, Integer.BYTES));
        maxLon = Math.max(maxLon, GeoEncodingUtils.decodeLongitude(maxPacked, Integer.BYTES));
      }
      if (minLat <= maxLat && minLon <= maxLon) {
        for (int i = 0; i < NUM_GEO_POINTS; ++i) {
          points.add(
              new double[] {
                minLat + random.nextDouble() * (maxLat - minLat),
                minLon + random.nextDouble() * (maxLon - minLon)
              });
        }
      }
    }
    return points;
  }

  private static double decodePoint(IndexableFieldDef<?> fieldDef, byte[] packed) {
    if (fieldDef instanceof IntFieldDef) {
      return NumericUtils.sortableBytesToInt(packed, 0);
    } else if (fieldDef instanceof FloatFieldDef) {
      return NumericUtils.sortableIntToFloat(NumericUtils.sortableBytesToInt(packed, 0));
    } else if (fieldDef instanceof DoubleFieldDef) {
      return NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(packed, 0));
    }
    // long and date time
    return NumericUtils.sortableBytesToLong(packed, 0);
  }

  private record TermAndFreq(String term, int docFreq) {}

  private Samples buildSamples(SchemaFields fields) {
    List<FieldSample> usable = fields.fieldSamples.stream().filter(this::hasValues).toList();
    Map<String, List<FieldSample>> byNestedPath = new LinkedHashMap<>();
    for (FieldSample sample : usable) {
      if (sample.nestedPath() != null) {
        byNestedPath.computeIfAbsent(sample.nestedPath(), k -> new ArrayList<>()).add(sample);
      }
    }
    List<FieldSample> textSamples =
        fields.textSamples.stream().filter(s -> !s.terms().isEmpty()).toList();
    List<GeoSample> geoSamples =
        fields.geoSamples.stream().filter(s -> !s.points().isEmpty()).toList();
    List<double[]> geoPoints = new ArrayList<>();
    geoSamples.forEach(s -> geoPoints.addAll(s.points()));
    return new Samples(
        usable,
        byNestedPath,
        textSamples,
        geoSamples,
        geoPoints,
        fields,
        scriptSources(fields.scriptFields));
  }

  private List<StructuredKind> applicableKinds(Samples samples) {
    List<StructuredKind> kinds = new ArrayList<>();
    if (samples.usable().size() >= 2) {
      kinds.add(StructuredKind.BOOLEAN);
    }
    if (!samples.fields().sortFields.isEmpty()) {
      kinds.add(StructuredKind.SORT);
    }
    if (!samples.textSamples().isEmpty()) {
      kinds.add(StructuredKind.TEXT);
    }
    // points are needed for a query center, the query needs a searchable field or a sort
    if (!samples.geoPoints().isEmpty()
        && (!samples.fields().polygonFields.isEmpty()
            || samples.geoSamples().stream()
                .anyMatch(s -> s.fieldDef().isSearchable() || s.fieldDef().hasDocValues()))) {
      kinds.add(StructuredKind.GEO);
    }
    if (samples.scriptSources().values().stream().anyMatch(sources -> !sources.isEmpty())) {
      kinds.add(StructuredKind.SCRIPT);
    }
    if (!samples.fields().aggregationFields.isEmpty()) {
      kinds.add(StructuredKind.AGGREGATION);
    }
    return kinds;
  }

  private void addFilterQueries(
      List<SearchRequest> requests,
      List<FieldSample> usable,
      List<String> docValuesFields,
      int numQueries) {
    if (usable.isEmpty() || numQueries <= 0) {
      return;
    }
    int numSingle = (int) (numQueries * SINGLE_FIELD_FRACTION);
    int numTwo = (int) (numQueries * TWO_FIELD_FRACTION);
    int numThree = numQueries - numSingle - numTwo;
    // a repeated query would mostly be answered from the query cache, so it warms little
    Set<Query> seen = new HashSet<>();
    int[] fieldIndex = {0};
    addUnique(
        requests,
        seen,
        docValuesFields,
        numSingle,
        () -> leafQuery(usable.get(fieldIndex[0]++ % usable.size())));
    if (usable.size() < 2) {
      return;
    }
    addUnique(requests, seen, docValuesFields, numTwo, () -> filterQuery(usable, 2));
    addUnique(requests, seen, docValuesFields, numThree, () -> filterQuery(usable, 3));
  }

  /** Add up to count requests with distinct queries, giving up after a bounded number of tries. */
  private void addUnique(
      List<SearchRequest> requests,
      Set<Query> seen,
      List<String> docValuesFields,
      int count,
      Supplier<Query> querySupplier) {
    int added = 0;
    for (int attempt = 0; attempt < count * MAX_ATTEMPTS_FACTOR && added < count; ++attempt) {
      Query query = querySupplier.get();
      if (!seen.add(query)) {
        continue;
      }
      SearchRequest.Builder builder = baseRequest().setQuery(query);
      addRetrieveFields(builder, docValuesFields, requests.size());
      requests.add(builder.build());
      added++;
    }
  }

  /**
   * Add the structured queries, splitting the count between the applicable kinds by weight.
   *
   * @return number of queries added per kind
   */
  private Map<StructuredKind, Integer> addStructuredQueries(
      List<SearchRequest> requests, Samples samples, List<StructuredKind> kinds, int count) {
    Map<StructuredKind, Integer> added = new EnumMap<>(StructuredKind.class);
    if (kinds.isEmpty() || count <= 0) {
      return added;
    }
    int totalWeight = kinds.stream().mapToInt(k -> k.weight).sum();
    Set<SearchRequest> seen = new HashSet<>();
    int assigned = 0;
    for (int i = 0; i < kinds.size(); ++i) {
      StructuredKind kind = kinds.get(i);
      int kindCount = i == kinds.size() - 1 ? count - assigned : count * kind.weight / totalWeight;
      assigned += kindCount;
      Supplier<SearchRequest.Builder> supplier =
          switch (kind) {
            case BOOLEAN -> () -> booleanRequest(samples);
            case SORT -> () -> sortRequest(samples);
            case TEXT -> () -> textRequest(samples);
            case GEO -> () -> geoRequest(samples);
            case SCRIPT -> () -> scriptRequest(samples);
            case AGGREGATION -> () -> aggregationRequest(samples);
          };
      added.put(
          kind,
          addUniqueRequests(requests, seen, samples.fields().docValuesFields, kindCount, supplier));
    }
    return added;
  }

  /**
   * Add up to count distinct requests, giving up after a bounded number of tries.
   *
   * @return number of requests added
   */
  private int addUniqueRequests(
      List<SearchRequest> requests,
      Set<SearchRequest> seen,
      List<String> docValuesFields,
      int count,
      Supplier<SearchRequest.Builder> requestSupplier) {
    int added = 0;
    for (int attempt = 0; attempt < count * MAX_ATTEMPTS_FACTOR && added < count; ++attempt) {
      SearchRequest.Builder builder = requestSupplier.get();
      if (!seen.add(builder.build())) {
        continue;
      }
      addRetrieveFields(builder, docValuesFields, requests.size());
      requests.add(builder.build());
      added++;
    }
    return added;
  }

  /**
   * Retrieve a random number of doc values fields, starting at a rotating offset, so every field's
   * values are loaded and the fetch cost of wide requests is warmed too.
   */
  private void addRetrieveFields(
      SearchRequest.Builder builder, List<String> docValuesFields, int offset) {
    if (docValuesFields.isEmpty()) {
      return;
    }
    int numFields = 1 + random.nextInt(docValuesFields.size());
    for (int j = 0; j < numFields; ++j) {
      builder.addRetrieveFields(docValuesFields.get((offset + j) % docValuesFields.size()));
    }
  }

  private boolean hasValues(FieldSample sample) {
    return !sample.terms().isEmpty() || sample.hasRange();
  }

  private SearchRequest.Builder baseRequest() {
    return baseRequest(TOP_HITS);
  }

  private SearchRequest.Builder baseRequest(int[] topHits) {
    int hits = topHits[random.nextInt(topHits.length)];
    if (maxTopHits > 0) {
      hits = Math.min(hits, maxTopHits);
    }
    return SearchRequest.newBuilder().setIndexName(indexState.getName()).setTopHits(hits);
  }

  private <T> T pick(List<T> values) {
    return values.get(random.nextInt(values.size()));
  }

  private static BooleanClause clause(BooleanClause.Occur occur, Query query) {
    return BooleanClause.newBuilder().setOccur(occur).setQuery(query).build();
  }

  private Query filterQuery(List<FieldSample> usable, int numClauses) {
    List<FieldSample> shuffled = new ArrayList<>(usable);
    Collections.shuffle(shuffled, random);
    BooleanQuery.Builder booleanQuery = BooleanQuery.newBuilder();
    for (int i = 0; i < numClauses && i < shuffled.size(); ++i) {
      booleanQuery.addClauses(clause(BooleanClause.Occur.FILTER, leafQuery(shuffled.get(i))));
    }
    return Query.newBuilder().setBooleanQuery(booleanQuery).build();
  }

  /**
   * Term or term set query on sampled values, or a range query over a random part of the value
   * range. Fields of nested documents are wrapped in a nested query.
   */
  @VisibleForTesting
  Query leafQuery(FieldSample sample) {
    Query query = unwrappedLeafQuery(sample);
    return sample.nestedPath() == null ? query : nestedQuery(sample.nestedPath(), query);
  }

  private Query nestedQuery(String path, Query query) {
    return Query.newBuilder()
        .setNestedQuery(
            NestedQuery.newBuilder()
                .setPath(path)
                .setQuery(query)
                .setScoreMode(NESTED_SCORE_MODES[random.nextInt(NESTED_SCORE_MODES.length)]))
        .build();
  }

  private Query unwrappedLeafQuery(FieldSample sample) {
    String field = sample.fieldDef().getName();
    if (sample.fieldDef() instanceof AtomFieldDef
        && sample.terms().size() >= 2
        && random.nextBoolean()) {
      return termSetQuery(sample);
    }
    if (!sample.terms().isEmpty()) {
      String value = sample.terms().get(random.nextInt(sample.terms().size()));
      TermQuery.Builder termQuery = TermQuery.newBuilder().setField(field);
      if (sample.fieldDef() instanceof BooleanFieldDef) {
        termQuery.setBooleanValue(Boolean.parseBoolean(value));
      } else {
        termQuery.setTextValue(value);
      }
      return Query.newBuilder().setTermQuery(termQuery).build();
    }
    double lower = sample.min() + random.nextDouble() * (sample.max() - sample.min());
    double upper = lower + random.nextDouble() * (sample.max() - lower);
    return Query.newBuilder()
        .setRangeQuery(
            RangeQuery.newBuilder()
                .setField(field)
                .setLower(formatBound(sample.fieldDef(), Math.floor(lower)))
                .setUpper(formatBound(sample.fieldDef(), Math.ceil(upper))))
        .build();
  }

  /** Term set query over a random subset of a text field's sampled values. */
  private Query termSetQuery(FieldSample sample) {
    List<String> shuffled = new ArrayList<>(sample.terms());
    Collections.shuffle(shuffled, random);
    int size = 2 + random.nextInt(Math.min(MAX_TERM_SET_SIZE, shuffled.size()) - 1);
    return Query.newBuilder()
        .setTermInSetQuery(
            TermInSetQuery.newBuilder()
                .setField(sample.fieldDef().getName())
                .setTextTerms(
                    TermInSetQuery.TextTerms.newBuilder().addAllTerms(shuffled.subList(0, size))))
        .build();
  }

  private static String formatBound(IndexableFieldDef<?> fieldDef, double value) {
    if (fieldDef instanceof DateTimeFieldDef dateTimeFieldDef) {
      String format = dateTimeFieldDef.getDateTimeFormat();
      if (format.equals(EPOCH_MILLIS)) {
        return Long.toString((long) value);
      }
      // range bounds are parsed as a LocalDateTime in UTC
      LocalDateTime dateTime =
          LocalDateTime.ofInstant(Instant.ofEpochMilli((long) value), ZoneOffset.UTC);
      return format.equals(STRICT_DATE_OPTIONAL_TIME)
          ? DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(dateTime)
          : DateTimeFormatter.ofPattern(format).format(dateTime);
    } else if (fieldDef instanceof IntFieldDef) {
      return Integer.toString((int) value);
    } else if (fieldDef instanceof LongFieldDef) {
      return Long.toString((long) value);
    }
    return Double.toString(value);
  }

  /** Filter query on one or two random fields, or no query if no field has sampled values. */
  private void setRandomFilter(SearchRequest.Builder builder, List<FieldSample> usable) {
    if (usable.isEmpty()) {
      return;
    }
    builder.setQuery(
        usable.size() >= 2 && random.nextBoolean()
            ? filterQuery(usable, 2)
            : leafQuery(pick(usable)));
  }

  private Query existsQuery(String field) {
    return Query.newBuilder().setExistsQuery(ExistsQuery.newBuilder().setField(field)).build();
  }

  /**
   * Boolean query mixing every occur type: a scored MUST clause, which is sometimes several clauses
   * of one nested document in a nested query, FILTER clauses, SHOULD clauses with a minimum to
   * match, MUST_NOT clauses including exists queries, and an inner boolean query.
   */
  private SearchRequest.Builder booleanRequest(Samples samples) {
    List<FieldSample> usable = samples.usable();
    List<String> existsFields = samples.fields().existsFields;
    BooleanQuery.Builder booleanQuery = BooleanQuery.newBuilder();
    if (!samples.usableByNestedPath().isEmpty() && random.nextBoolean()) {
      String path = pick(new ArrayList<>(samples.usableByNestedPath().keySet()));
      List<FieldSample> nestedFields = samples.usableByNestedPath().get(path);
      BooleanQuery.Builder inner = BooleanQuery.newBuilder();
      int numClauses = 1 + random.nextInt(Math.min(3, nestedFields.size()));
      for (int i = 0; i < numClauses; ++i) {
        inner.addClauses(
            clause(
                i == 0 ? BooleanClause.Occur.MUST : BooleanClause.Occur.FILTER,
                unwrappedLeafQuery(pick(nestedFields))));
      }
      booleanQuery.addClauses(
          clause(
              BooleanClause.Occur.MUST,
              nestedQuery(path, Query.newBuilder().setBooleanQuery(inner).build())));
    } else {
      booleanQuery.addClauses(clause(BooleanClause.Occur.MUST, leafQuery(pick(usable))));
    }
    int numFilters = 1 + random.nextInt(3);
    for (int i = 0; i < numFilters; ++i) {
      booleanQuery.addClauses(clause(BooleanClause.Occur.FILTER, leafQuery(pick(usable))));
    }
    if (random.nextBoolean()) {
      int numShould = 2 + random.nextInt(3);
      for (int i = 0; i < numShould; ++i) {
        booleanQuery.addClauses(clause(BooleanClause.Occur.SHOULD, leafQuery(pick(usable))));
      }
      if (random.nextBoolean()) {
        booleanQuery.setMinimumNumberShouldMatch(1 + random.nextInt(2));
      }
    }
    int numMustNot = random.nextInt(3);
    for (int i = 0; i < numMustNot; ++i) {
      Query query =
          !existsFields.isEmpty() && random.nextBoolean()
              ? existsQuery(pick(existsFields))
              : leafQuery(pick(usable));
      booleanQuery.addClauses(clause(BooleanClause.Occur.MUST_NOT, query));
    }
    if (!existsFields.isEmpty() && random.nextBoolean()) {
      booleanQuery.addClauses(clause(BooleanClause.Occur.FILTER, existsQuery(pick(existsFields))));
    }
    if (random.nextBoolean()) {
      BooleanQuery.Builder inner = BooleanQuery.newBuilder();
      inner.addClauses(clause(BooleanClause.Occur.SHOULD, leafQuery(pick(usable))));
      inner.addClauses(clause(BooleanClause.Occur.SHOULD, leafQuery(pick(usable))));
      booleanQuery.addClauses(
          clause(BooleanClause.Occur.FILTER, Query.newBuilder().setBooleanQuery(inner).build()));
    }
    return baseRequest(LARGE_TOP_HITS)
        .setQuery(Query.newBuilder().setBooleanQuery(booleanQuery).build());
  }

  /** Filter query sorted by one or two doc values fields. */
  private SearchRequest.Builder sortRequest(Samples samples) {
    SearchRequest.Builder builder = baseRequest(LARGE_TOP_HITS);
    setRandomFilter(builder, samples.usable());
    List<String> sortFields = new ArrayList<>(samples.fields().sortFields);
    Collections.shuffle(sortFields, random);
    SortFields.Builder sort = SortFields.newBuilder();
    int numSortFields = Math.min(sortFields.size(), 1 + random.nextInt(2));
    for (int i = 0; i < numSortFields; ++i) {
      sort.addSortedFields(
          SortType.newBuilder()
              .setFieldName(sortFields.get(i))
              .setReverse(random.nextBoolean())
              .setMissingLast(random.nextBoolean()));
    }
    return builder.setQuerySort(QuerySortField.newBuilder().setFields(sort));
  }

  /** Match, match phrase or multi match query, sometimes combined with filters. */
  private SearchRequest.Builder textRequest(Samples samples) {
    List<FieldSample> textSamples = samples.textSamples();
    FieldSample sample = pick(textSamples);
    String field = sample.fieldDef().getName();
    int numTerms = 1 + random.nextInt(3);
    MatchOperator operator =
        numTerms <= 2 && random.nextBoolean() ? MatchOperator.MUST : MatchOperator.SHOULD;
    Query textQuery;
    int type = random.nextInt(3);
    if (type == 1) {
      textQuery =
          Query.newBuilder()
              .setMatchPhraseQuery(
                  MatchPhraseQuery.newBuilder()
                      .setField(field)
                      .setQuery(sampleQueryText(sample, 2))
                      .setSlop(random.nextInt(3)))
              .build();
    } else if (type == 2 && textSamples.size() >= 2) {
      List<FieldSample> shuffled = new ArrayList<>(textSamples);
      Collections.shuffle(shuffled, random);
      MultiMatchQuery.Builder multiMatch =
          MultiMatchQuery.newBuilder()
              .setQuery(sampleQueryText(sample, numTerms))
              .setOperator(operator);
      int numFields = Math.min(shuffled.size(), 2 + random.nextInt(2));
      for (int i = 0; i < numFields; ++i) {
        multiMatch.addFields(shuffled.get(i).fieldDef().getName());
      }
      textQuery = Query.newBuilder().setMultiMatchQuery(multiMatch).build();
    } else {
      MatchQuery.Builder match =
          MatchQuery.newBuilder()
              .setField(field)
              .setQuery(sampleQueryText(sample, numTerms))
              .setOperator(operator);
      if (operator == MatchOperator.SHOULD && numTerms >= 2 && random.nextBoolean()) {
        match.setMinimumNumberShouldMatch(1 + random.nextInt(numTerms - 1));
      }
      textQuery = Query.newBuilder().setMatchQuery(match).build();
    }
    SearchRequest.Builder builder = baseRequest(LARGE_TOP_HITS);
    if (!samples.usable().isEmpty() && random.nextBoolean()) {
      BooleanQuery.Builder booleanQuery =
          BooleanQuery.newBuilder().addClauses(clause(BooleanClause.Occur.MUST, textQuery));
      int numFilters = 1 + random.nextInt(2);
      for (int i = 0; i < numFilters; ++i) {
        booleanQuery.addClauses(
            clause(BooleanClause.Occur.FILTER, leafQuery(pick(samples.usable()))));
      }
      return builder.setQuery(Query.newBuilder().setBooleanQuery(booleanQuery).build());
    }
    return builder.setQuery(textQuery);
  }

  private String sampleQueryText(FieldSample sample, int numTerms) {
    List<String> terms = new ArrayList<>(numTerms);
    for (int i = 0; i < numTerms; ++i) {
      terms.add(pick(sample.terms()));
    }
    return String.join(" ", terms);
  }

  /**
   * Geo radius or bounding box query around a sampled point, and point in polygon query, sometimes
   * sorted by distance from the point and combined with a filter.
   */
  private SearchRequest.Builder geoRequest(Samples samples) {
    SearchRequest.Builder builder = baseRequest(LARGE_TOP_HITS);
    BooleanQuery.Builder booleanQuery = BooleanQuery.newBuilder();
    List<GeoSample> searchable =
        samples.geoSamples().stream().filter(s -> s.fieldDef().isSearchable()).toList();
    List<GeoSample> sortable =
        samples.geoSamples().stream().filter(s -> s.fieldDef().hasDocValues()).toList();
    List<PolygonfieldDef> polygonFields = samples.fields().polygonFields;
    double[] point;
    if (!searchable.isEmpty() && (polygonFields.isEmpty() || random.nextBoolean())) {
      GeoSample sample = pick(searchable);
      point = pick(sample.points());
      String field = sample.fieldDef().getName();
      Query geoQuery;
      if (random.nextInt(10) < 7) {
        geoQuery =
            Query.newBuilder()
                .setGeoRadiusQuery(
                    GeoRadiusQuery.newBuilder()
                        .setField(field)
                        .setCenter(latLng(point[0], point[1]))
                        .setRadius(GEO_RADII[random.nextInt(GEO_RADII.length)]))
                .build();
      } else {
        double halfSize = GEO_BOX_HALF_SIZES[random.nextInt(GEO_BOX_HALF_SIZES.length)];
        geoQuery =
            Query.newBuilder()
                .setGeoBoundingBoxQuery(
                    GeoBoundingBoxQuery.newBuilder()
                        .setField(field)
                        .setTopLeft(
                            latLng(
                                Math.min(90, point[0] + halfSize),
                                Math.max(-180, point[1] - halfSize)))
                        .setBottomRight(
                            latLng(
                                Math.max(-90, point[0] - halfSize),
                                Math.min(180, point[1] + halfSize))))
                .build();
      }
      booleanQuery.addClauses(clause(BooleanClause.Occur.FILTER, geoQuery));
    } else {
      point = pick(samples.geoPoints());
    }
    if (!polygonFields.isEmpty() && (booleanQuery.getClausesCount() == 0 || random.nextBoolean())) {
      booleanQuery.addClauses(
          clause(
              BooleanClause.Occur.FILTER,
              Query.newBuilder()
                  .setGeoPointQuery(
                      GeoPointQuery.newBuilder()
                          .setField(pick(polygonFields).getName())
                          .setPoint(latLng(point[0], point[1])))
                  .build()));
    }
    if (!samples.usable().isEmpty() && random.nextBoolean()) {
      booleanQuery.addClauses(
          clause(BooleanClause.Occur.FILTER, leafQuery(pick(samples.usable()))));
    }
    if (booleanQuery.getClausesCount() > 0) {
      builder.setQuery(Query.newBuilder().setBooleanQuery(booleanQuery).build());
    }
    if (!sortable.isEmpty() && (booleanQuery.getClausesCount() == 0 || random.nextInt(10) < 7)) {
      builder.setQuerySort(
          QuerySortField.newBuilder()
              .setFields(
                  SortFields.newBuilder()
                      .addSortedFields(
                          SortType.newBuilder()
                              .setFieldName(pick(sortable).fieldDef().getName())
                              .setOrigin(
                                  Point.newBuilder()
                                      .setLatitude(point[0])
                                      .setLongitude(point[1])))));
    }
    return builder;
  }

  private static LatLng latLng(double latitude, double longitude) {
    return LatLng.newBuilder().setLatitude(latitude).setLongitude(longitude).build();
  }

  /**
   * Script sources per language over single valued numeric doc values fields. Painless is only used
   * when the painless plugin is installed, js only with fields named as js variables.
   */
  private Map<String, List<String>> scriptSources(List<String> scriptFields) {
    Map<String, List<String>> sources = new LinkedHashMap<>();
    if (scriptFields.isEmpty()) {
      return sources;
    }
    ScriptService scriptService = ScriptService.getInstance();
    if (scriptService != null && scriptService.hasScriptEngine(PAINLESS_LANG)) {
      sources.put(PAINLESS_LANG, buildSources(scriptFields, this::painlessSource));
    }
    List<String> jsFields =
        scriptFields.stream().filter(f -> JS_VARIABLE.matcher(f).matches()).toList();
    if (!jsFields.isEmpty()) {
      sources.put(JS_LANG, buildSources(jsFields, this::jsSource));
    }
    return sources;
  }

  private List<String> buildSources(
      List<String> fields, java.util.function.Function<List<String>, String> toSource) {
    Set<String> sources = new LinkedHashSet<>();
    for (int attempt = 0;
        attempt < MAX_SCRIPT_SOURCES * MAX_ATTEMPTS_FACTOR && sources.size() < MAX_SCRIPT_SOURCES;
        ++attempt) {
      List<String> shuffled = new ArrayList<>(fields);
      Collections.shuffle(shuffled, random);
      int numFields = 1 + random.nextInt(Math.min(MAX_SCRIPT_FIELDS, shuffled.size()));
      sources.add(toSource.apply(shuffled.subList(0, numFields)));
    }
    return new ArrayList<>(sources);
  }

  private String painlessSource(List<String> fields) {
    StringBuilder source = new StringBuilder("double v = 0.0;");
    for (String field : fields) {
      source
          .append(" if (doc['")
          .append(field)
          .append("'].size() > 0) { v += doc['")
          .append(field)
          .append("'].value * ")
          .append(SCRIPT_WEIGHTS[random.nextInt(SCRIPT_WEIGHTS.length)])
          .append("; }");
    }
    return source.append(" return v;").toString();
  }

  private String jsSource(List<String> fields) {
    List<String> terms = new ArrayList<>(fields.size());
    for (String field : fields) {
      terms.add(field + " * " + SCRIPT_WEIGHTS[random.nextInt(SCRIPT_WEIGHTS.length)]);
    }
    return String.join(" + ", terms);
  }

  /** Filter query with virtual fields computed by scripts, retrieved and sometimes sorted by. */
  private SearchRequest.Builder scriptRequest(Samples samples) {
    List<String> langs =
        samples.scriptSources().entrySet().stream()
            .filter(e -> !e.getValue().isEmpty())
            .map(Map.Entry::getKey)
            .toList();
    // live queries use painless, js is only the fallback without the plugin
    String lang = langs.contains(PAINLESS_LANG) ? PAINLESS_LANG : pick(langs);
    List<String> sources = samples.scriptSources().get(lang);
    SearchRequest.Builder builder = baseRequest(LARGE_TOP_HITS);
    setRandomFilter(builder, samples.usable());
    int numVirtualFields = 1 + random.nextInt(MAX_SCRIPT_FIELDS);
    for (int i = 0; i < numVirtualFields; ++i) {
      String name = VIRTUAL_FIELD_PREFIX + i;
      builder.addVirtualFields(
          VirtualField.newBuilder()
              .setName(name)
              .setScript(Script.newBuilder().setLang(lang).setSource(pick(sources))));
      builder.addRetrieveFields(name);
    }
    if (random.nextBoolean()) {
      builder.setQuerySort(
          QuerySortField.newBuilder()
              .setFields(
                  SortFields.newBuilder()
                      .addSortedFields(
                          SortType.newBuilder()
                              .setFieldName(VIRTUAL_FIELD_PREFIX + 0)
                              .setReverse(random.nextBoolean()))));
    }
    return builder;
  }

  /** Filter query with one or two terms collectors, some with a nested top hits collector. */
  private SearchRequest.Builder aggregationRequest(Samples samples) {
    SearchRequest.Builder builder = baseRequest(new int[] {0, 10});
    setRandomFilter(builder, samples.usable());
    List<String> fields = new ArrayList<>(samples.fields().aggregationFields);
    Collections.shuffle(fields, random);
    int numCollectors = Math.min(fields.size(), 1 + random.nextInt(2));
    for (int i = 0; i < numCollectors; ++i) {
      Collector.Builder collector =
          Collector.newBuilder()
              .setTerms(
                  TermsCollector.newBuilder()
                      .setField(fields.get(i))
                      .setSize(
                          TERMS_COLLECTOR_SIZES[random.nextInt(TERMS_COLLECTOR_SIZES.length)]));
      if (random.nextBoolean()) {
        collector.putNestedCollectors(
            NESTED_COLLECTOR_NAME,
            Collector.newBuilder()
                .setTopHitsCollector(TopHitsCollector.newBuilder().setTopHits(NESTED_TOP_HITS))
                .build());
      }
      builder.putCollectors(COLLECTOR_PREFIX + i, collector.build());
    }
    return builder;
  }

  private void addFacetQueries(List<SearchRequest> requests, List<IndexableFieldDef<?>> fields) {
    for (int i = 0; i < fields.size(); i += FACET_FIELDS_PER_REQUEST) {
      SearchRequest.Builder builder = baseRequest().setTopHits(0);
      for (IndexableFieldDef<?> field :
          fields.subList(i, Math.min(i + FACET_FIELDS_PER_REQUEST, fields.size()))) {
        builder.addFacets(Facet.newBuilder().setDim(field.getName()).setTopN(FACET_TOP_N));
      }
      requests.add(builder.build());
    }
  }

  /** Vectors read from the index for a vector field, used as kNN query vectors. */
  @VisibleForTesting
  record VectorSeeds(VectorFieldDef<?> fieldDef, List<float[]> floats, List<byte[]> bytes) {}

  @VisibleForTesting
  static VectorSeeds sampleVectors(IndexSearcher searcher, VectorFieldDef<?> fieldDef)
      throws IOException {
    String field = fieldDef.getName();
    boolean isByte = fieldDef instanceof VectorFieldDef.ByteVectorFieldDef;
    List<float[]> floats = new ArrayList<>();
    List<byte[]> bytes = new ArrayList<>();
    for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
      KnnVectorValues values =
          isByte
              ? leaf.reader().getByteVectorValues(field)
              : leaf.reader().getFloatVectorValues(field);
      if (values == null) {
        continue;
      }
      KnnVectorValues.DocIndexIterator iterator = values.iterator();
      while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS
          && floats.size() + bytes.size() < NUM_VECTOR_SEEDS) {
        if (isByte) {
          bytes.add(((ByteVectorValues) values).vectorValue(iterator.index()).clone());
        } else {
          floats.add(((FloatVectorValues) values).vectorValue(iterator.index()).clone());
        }
      }
      if (floats.size() + bytes.size() >= NUM_VECTOR_SEEDS) {
        break;
      }
    }
    return new VectorSeeds(fieldDef, floats, bytes);
  }

  private void addKnnQueries(List<SearchRequest> requests, VectorSeeds seeds) {
    boolean isByte = seeds.fieldDef() instanceof VectorFieldDef.ByteVectorFieldDef;
    int dimensions = seeds.fieldDef().getVectorDimensions();
    for (int i = 0; i < KNN_QUERIES_PER_FIELD; ++i) {
      KnnQuery.Builder knnQuery =
          KnnQuery.newBuilder()
              .setField(seeds.fieldDef().getName())
              .setK(KNN_K)
              .setNumCandidates(KNN_NUM_CANDIDATES);
      if (isByte) {
        byte[] vector =
            seeds.bytes().isEmpty()
                ? randomBytes(dimensions)
                : seeds.bytes().get(random.nextInt(seeds.bytes().size()));
        knnQuery.setQueryByteVector(ByteString.copyFrom(vector));
      } else {
        float[] vector =
            seeds.floats().isEmpty()
                ? randomUnitVector(dimensions)
                : seeds.floats().get(random.nextInt(seeds.floats().size()));
        for (float v : vector) {
          knnQuery.addQueryVector(v);
        }
      }
      requests.add(baseRequest().addKnn(knnQuery).build());
    }
  }

  // random vectors still traverse the graph, they are normalized so cosine/dot product accept them
  private float[] randomUnitVector(int dimensions) {
    float[] vector = new float[dimensions];
    double norm = 0;
    for (int i = 0; i < dimensions; ++i) {
      vector[i] = (float) random.nextGaussian();
      norm += vector[i] * vector[i];
    }
    norm = Math.sqrt(norm);
    for (int i = 0; i < dimensions; ++i) {
      vector[i] /= (float) norm;
    }
    return vector;
  }

  private byte[] randomBytes(int dimensions) {
    byte[] vector = new byte[dimensions];
    random.nextBytes(vector);
    return vector;
  }
}
