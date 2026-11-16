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
import com.yelp.nrtsearch.server.field.AtomFieldDef;
import com.yelp.nrtsearch.server.field.BooleanFieldDef;
import com.yelp.nrtsearch.server.field.DateTimeFieldDef;
import com.yelp.nrtsearch.server.field.DoubleFieldDef;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.FloatFieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef.FacetValueType;
import com.yelp.nrtsearch.server.field.IntFieldDef;
import com.yelp.nrtsearch.server.field.LongFieldDef;
import com.yelp.nrtsearch.server.field.VectorFieldDef;
import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.BooleanQuery;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.ReaderUtil;
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
 * ranges, stored vectors), so the queries match real documents and load the index blocks live
 * traffic uses. Text values are read from randomly chosen documents, so common values are picked as
 * often as they occur. Falls back to synthetic values only when a field has nothing to sample.
 * Filter queries vary topHits and retrieve a rotating subset of doc values fields, so the fetch
 * phase is warmed as well, and no query is generated twice.
 *
 * <p>Queries are generated in stages:
 *
 * <ol>
 *   <li>Single field (60%): one term, term set or range query per searchable field, cycling all
 *       fields.
 *   <li>Two field filters (30%): pairs of random fields as FILTER clauses.
 *   <li>Three field filters (10%): triples of random fields as FILTER clauses.
 *   <li>Facets: facet-only requests over every faceted field, in groups.
 *   <li>Vectors: kNN queries per vector field, seeded with vectors read from the index.
 * </ol>
 *
 * <p>Only base retrieval is warmed. No rescorers, scripts or plugin queries are generated, so this
 * does not warm model inference paths.
 */
public class SchemaWarmingQueryGenerator {
  private static final Logger logger = LoggerFactory.getLogger(SchemaWarmingQueryGenerator.class);

  private static final String EPOCH_MILLIS = "epoch_millis";
  private static final String STRICT_DATE_OPTIONAL_TIME = "strict_date_optional_time";
  // topHits of generated filter queries, cycled so collection of small and larger result sets is
  // warmed, all below the default maxQueryTopHits guardrail
  static final int[] TOP_HITS = {10, 30, 100};
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
  static final int FACET_FIELDS_PER_REQUEST = 5;
  static final int FACET_TOP_N = 10;
  static final int NUM_VECTOR_SEEDS = 100;
  static final int KNN_QUERIES_PER_FIELD = 20;
  static final int KNN_K = 100;
  static final int KNN_NUM_CANDIDATES = 200;
  static final double SINGLE_FIELD_FRACTION = 0.6;
  static final double TWO_FIELD_FRACTION = 0.3;

  private final IndexState indexState;
  private final Random random;

  /** A searchable field, with values sampled from the index to build queries from. */
  @VisibleForTesting
  record FieldSample(IndexableFieldDef<?> fieldDef, List<String> terms, double min, double max) {
    boolean hasRange() {
      return min <= max;
    }
  }

  public SchemaWarmingQueryGenerator(IndexState indexState) {
    this(indexState, new Random());
  }

  @VisibleForTesting
  SchemaWarmingQueryGenerator(IndexState indexState, Random random) {
    this.indexState = indexState;
    this.random = random;
  }

  /**
   * Generate warming queries for the index.
   *
   * @param numQueries number of term and range queries to generate, facet and vector queries are
   *     added on top of this
   * @return generated queries, empty if the index has no searchable fields
   */
  public List<SearchRequest> generate(int numQueries) throws IOException {
    List<FieldSample> fieldSamples = new ArrayList<>();
    List<IndexableFieldDef<?>> facetFields = new ArrayList<>();
    List<VectorFieldDef<?>> vectorFields = new ArrayList<>();
    List<String> docValuesFields = new ArrayList<>();
    for (FieldDef fieldDef : indexState.getAllFields().values()) {
      // child fields (name.child) are part of their parent, nested fields need a nested query
      if (!(fieldDef instanceof IndexableFieldDef<?> indexableFieldDef)
          || fieldDef.getName().contains(".")) {
        continue;
      }
      if (indexableFieldDef instanceof VectorFieldDef<?> vectorFieldDef) {
        if (vectorFieldDef.isSearchable()) {
          vectorFields.add(vectorFieldDef);
        }
        continue;
      }
      if (indexableFieldDef.hasDocValues()) {
        docValuesFields.add(fieldDef.getName());
      }
      FacetValueType facetValueType = indexableFieldDef.getFacetValueType();
      if (facetValueType != FacetValueType.NO_FACETS
          && facetValueType != FacetValueType.NUMERIC_RANGE) {
        facetFields.add(indexableFieldDef);
      }
      if (indexableFieldDef.isSearchable() && isSupported(indexableFieldDef)) {
        fieldSamples.add(new FieldSample(indexableFieldDef, List.of(), 1, 0));
      }
    }

    List<VectorSeeds> vectorSeeds = new ArrayList<>();
    ShardState shardState = indexState.getShard(0);
    SearcherAndTaxonomy searcherAndTaxonomy = shardState.acquire();
    try {
      IndexSearcher searcher = searcherAndTaxonomy.searcher();
      for (int i = 0; i < fieldSamples.size(); ++i) {
        fieldSamples.set(i, sampleField(searcher, fieldSamples.get(i).fieldDef(), random));
      }
      for (VectorFieldDef<?> vectorField : vectorFields) {
        vectorSeeds.add(sampleVectors(searcher, vectorField));
      }
    } finally {
      shardState.release(searcherAndTaxonomy);
    }

    List<SearchRequest> requests = new ArrayList<>();
    addFilterQueries(requests, fieldSamples, docValuesFields, numQueries);
    addFacetQueries(requests, facetFields);
    for (VectorSeeds seeds : vectorSeeds) {
      addKnnQueries(requests, seeds);
    }
    logger.info(
        "Generated {} schema warming queries for index: {} from {} searchable, {} facet and {}"
            + " vector fields",
        requests.size(),
        indexState.getName(),
        fieldSamples.size(),
        facetFields.size(),
        vectorFields.size());
    return requests;
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
      // doc values iterators only move forward, so use a new one for each random document
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
    }
    return new ArrayList<>(values);
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

  private void addFilterQueries(
      List<SearchRequest> requests,
      List<FieldSample> fieldSamples,
      List<String> docValuesFields,
      int numQueries) {
    List<FieldSample> usable = fieldSamples.stream().filter(this::hasValues).toList();
    if (usable.isEmpty()) {
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
      java.util.function.Supplier<Query> querySupplier) {
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
    return SearchRequest.newBuilder()
        .setIndexName(indexState.getName())
        .setTopHits(TOP_HITS[random.nextInt(TOP_HITS.length)]);
  }

  private Query filterQuery(List<FieldSample> usable, int numClauses) {
    List<FieldSample> shuffled = new ArrayList<>(usable);
    Collections.shuffle(shuffled, random);
    BooleanQuery.Builder booleanQuery = BooleanQuery.newBuilder();
    for (int i = 0; i < numClauses && i < shuffled.size(); ++i) {
      booleanQuery.addClauses(
          BooleanClause.newBuilder()
              .setOccur(BooleanClause.Occur.FILTER)
              .setQuery(leafQuery(shuffled.get(i))));
    }
    return Query.newBuilder().setBooleanQuery(booleanQuery).build();
  }

  /** Term query on a sampled value, or a range query over a random part of the value range. */
  @VisibleForTesting
  Query leafQuery(FieldSample sample) {
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
