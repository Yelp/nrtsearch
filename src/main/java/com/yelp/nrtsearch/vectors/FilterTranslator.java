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
package com.yelp.nrtsearch.vectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.BooleanQuery;
import com.yelp.nrtsearch.server.grpc.ExistsQuery;
import com.yelp.nrtsearch.server.grpc.MatchAllQuery;
import com.yelp.nrtsearch.server.grpc.PrefixQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.vectors.MetadataSchema.MetadataType;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Translates an S3 Vectors metadata filter (a JSON document) into an nrtsearch {@link Query}.
 *
 * <p>Supported operators: {@code $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $startsWith,
 * $and, $or}. A bare value means {@code $eq}. Several operators on the same key, or several keys in
 * one object, are combined with AND. When matched against a list value, {@code $eq} and {@code $in}
 * are true if any element matches.
 */
public class FilterTranslator {
  public static final int MAX_FILTER_CONSTRAINTS = 100;

  private final IndexConfig config;
  private final MetadataSchema schema;
  private int constraints;

  private FilterTranslator(IndexConfig config) {
    this.config = config;
    this.schema = config.getMetadataSchema();
  }

  /**
   * Translate a filter.
   *
   * @param filter filter JSON, must be an object
   * @param config index config, used for key types and filterability
   * @return nrtsearch query matching the filter
   * @throws VectorsException if the filter is invalid
   */
  public static Query translate(JsonNode filter, IndexConfig config) {
    if (filter == null || !filter.isObject()) {
      throw VectorsException.validation("filter must be a JSON object");
    }
    FilterTranslator translator = new FilterTranslator(config);
    Query query = translator.translateObject(filter);
    if (translator.constraints > MAX_FILTER_CONSTRAINTS) {
      throw VectorsException.validation(
          "filter has "
              + translator.constraints
              + " constraints, the maximum is "
              + MAX_FILTER_CONSTRAINTS);
    }
    return query;
  }

  /** An object is an AND of its entries. */
  private Query translateObject(JsonNode node) {
    if (node.isEmpty()) {
      throw VectorsException.validation("filter objects must not be empty");
    }
    List<Query> parts = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> it = node.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      String name = entry.getKey();
      JsonNode value = entry.getValue();
      switch (name) {
        case "$and" -> parts.add(translateLogical(value, BooleanClause.Occur.FILTER, name));
        case "$or" -> parts.add(translateLogical(value, BooleanClause.Occur.SHOULD, name));
        default -> {
          if (name.startsWith("$")) {
            throw VectorsException.validation("Unknown filter operator at top level: " + name);
          }
          parts.add(translateKey(name, value));
        }
      }
    }
    return and(parts);
  }

  private Query translateLogical(JsonNode value, BooleanClause.Occur occur, String op) {
    if (!value.isArray() || value.isEmpty()) {
      throw VectorsException.validation(op + " requires a non-empty array of filters");
    }
    BooleanQuery.Builder builder = BooleanQuery.newBuilder();
    for (JsonNode child : value) {
      if (!child.isObject()) {
        throw VectorsException.validation(op + " array elements must be filter objects");
      }
      builder.addClauses(clause(translateObject(child), occur));
    }
    if (occur == BooleanClause.Occur.SHOULD) {
      builder.setMinimumNumberShouldMatch(1);
    }
    return Query.newBuilder().setBooleanQuery(builder).build();
  }

  private Query translateKey(String key, JsonNode value) {
    if (!config.isFilterable(key)) {
      throw VectorsException.validation(
          "Metadata key '" + key + "' is non-filterable and cannot be used in a filter");
    }
    if (value.isObject()) {
      if (value.isEmpty()) {
        throw VectorsException.validation("Operator object for key '" + key + "' is empty");
      }
      List<Query> parts = new ArrayList<>();
      Iterator<Map.Entry<String, JsonNode>> it = value.fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> entry = it.next();
        parts.add(translateOperator(key, entry.getKey(), entry.getValue()));
      }
      return and(parts);
    }
    return translateOperator(key, "$eq", value);
  }

  private Query translateOperator(String key, String op, JsonNode operand) {
    return switch (op) {
      case "$eq" -> {
        requirePrimitive(key, op, operand);
        constraints++;
        yield eq(key, operand);
      }
      case "$ne" -> {
        requirePrimitive(key, op, operand);
        constraints++;
        // Vectors that don't have the key also match $ne
        yield not(eq(key, operand));
      }
      case "$gt", "$gte", "$lt", "$lte" -> {
        if (!operand.isNumber()) {
          throw VectorsException.validation(op + " on key '" + key + "' requires a number");
        }
        constraints++;
        yield range(key, op, operand.asDouble());
      }
      case "$in" -> {
        requirePrimitiveArray(key, op, operand);
        constraints += operand.size();
        yield in(key, operand);
      }
      case "$nin" -> {
        requirePrimitiveArray(key, op, operand);
        constraints += operand.size();
        yield not(in(key, operand));
      }
      case "$exists" -> {
        if (!operand.isBoolean()) {
          throw VectorsException.validation("$exists on key '" + key + "' requires a boolean");
        }
        constraints++;
        Query exists = exists(key);
        yield operand.asBoolean() ? exists : not(exists);
      }
      case "$startsWith" -> {
        if (!operand.isTextual()) {
          throw VectorsException.validation("$startsWith on key '" + key + "' requires a string");
        }
        constraints++;
        yield startsWith(key, operand.asText());
      }
      default -> throw VectorsException.validation("Unknown filter operator: " + op);
    };
  }

  private Query eq(String key, JsonNode operand) {
    String field = schema.getFieldName(key);
    MetadataType type = schema.getType(key);
    MetadataType operandType = MetadataSchema.inferType(key, operand);
    if (field == null || type != operandType) {
      // no vector has this key with a value of this type
      return matchNone();
    }
    TermQuery.Builder term = TermQuery.newBuilder().setField(field);
    switch (type) {
      case STRING -> term.setTextValue(operand.asText());
      case NUMBER -> term.setDoubleValue(operand.asDouble());
      case BOOLEAN -> term.setBooleanValue(operand.asBoolean());
    }
    return Query.newBuilder().setTermQuery(term).build();
  }

  private Query in(String key, JsonNode operand) {
    String field = schema.getFieldName(key);
    MetadataType type = schema.getType(key);
    if (field == null) {
      return matchNone();
    }
    // Keep only the values whose type matches the key type
    List<JsonNode> values = new ArrayList<>();
    for (JsonNode element : operand) {
      if (MetadataSchema.inferType(key, element) == type) {
        values.add(element);
      }
    }
    if (values.isEmpty()) {
      return matchNone();
    }
    TermInSetQuery.Builder builder = TermInSetQuery.newBuilder().setField(field);
    switch (type) {
      case STRING -> {
        TermInSetQuery.TextTerms.Builder terms = TermInSetQuery.TextTerms.newBuilder();
        values.forEach(v -> terms.addTerms(v.asText()));
        builder.setTextTerms(terms);
      }
      case NUMBER -> {
        TermInSetQuery.DoubleTerms.Builder terms = TermInSetQuery.DoubleTerms.newBuilder();
        values.forEach(v -> terms.addTerms(v.asDouble()));
        builder.setDoubleTerms(terms);
      }
      case BOOLEAN -> {
        // TermInSetQuery has no boolean variant; use a disjunction of terms
        List<Query> terms = new ArrayList<>();
        values.forEach(v -> terms.add(eq(key, v)));
        return or(terms);
      }
    }
    return Query.newBuilder().setTermInSetQuery(builder).build();
  }

  private Query range(String key, String op, double bound) {
    String field = schema.getFieldName(key);
    if (field == null || schema.getType(key) != MetadataType.NUMBER) {
      return matchNone();
    }
    RangeQuery.Builder range = RangeQuery.newBuilder().setField(field);
    String value = Double.toString(bound);
    switch (op) {
      case "$gt" -> range.setLower(value).setLowerExclusive(true);
      case "$gte" -> range.setLower(value);
      case "$lt" -> range.setUpper(value).setUpperExclusive(true);
      case "$lte" -> range.setUpper(value);
      default -> throw new IllegalStateException(op);
    }
    return Query.newBuilder().setRangeQuery(range).build();
  }

  private Query exists(String key) {
    String field = schema.getFieldName(key);
    if (field == null) {
      return matchNone();
    }
    return Query.newBuilder().setExistsQuery(ExistsQuery.newBuilder().setField(field)).build();
  }

  private Query startsWith(String key, String prefix) {
    String field = schema.getFieldName(key);
    if (field == null || schema.getType(key) != MetadataType.STRING) {
      return matchNone();
    }
    return Query.newBuilder()
        .setPrefixQuery(PrefixQuery.newBuilder().setField(field).setPrefix(prefix))
        .build();
  }

  private static void requirePrimitive(String key, String op, JsonNode operand) {
    if (!(operand.isTextual() || operand.isNumber() || operand.isBoolean())) {
      throw VectorsException.validation(
          op + " on key '" + key + "' requires a string, number or boolean");
    }
  }

  private static void requirePrimitiveArray(String key, String op, JsonNode operand) {
    if (!operand.isArray() || operand.isEmpty()) {
      throw VectorsException.validation(op + " on key '" + key + "' requires a non-empty array");
    }
    for (JsonNode element : operand) {
      requirePrimitive(key, op, element);
    }
  }

  static Query matchAll() {
    return Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder()).build();
  }

  static Query matchNone() {
    return not(matchAll());
  }

  /** Documents that do not match the query. */
  static Query not(Query query) {
    return Query.newBuilder()
        .setBooleanQuery(
            BooleanQuery.newBuilder()
                .addClauses(clause(matchAll(), BooleanClause.Occur.FILTER))
                .addClauses(clause(query, BooleanClause.Occur.MUST_NOT)))
        .build();
  }

  private static Query and(List<Query> parts) {
    if (parts.size() == 1) {
      return parts.get(0);
    }
    BooleanQuery.Builder builder = BooleanQuery.newBuilder();
    parts.forEach(p -> builder.addClauses(clause(p, BooleanClause.Occur.FILTER)));
    return Query.newBuilder().setBooleanQuery(builder).build();
  }

  private static Query or(List<Query> parts) {
    if (parts.size() == 1) {
      return parts.get(0);
    }
    BooleanQuery.Builder builder = BooleanQuery.newBuilder().setMinimumNumberShouldMatch(1);
    parts.forEach(p -> builder.addClauses(clause(p, BooleanClause.Occur.SHOULD)));
    return Query.newBuilder().setBooleanQuery(builder).build();
  }

  private static BooleanClause clause(Query query, BooleanClause.Occur occur) {
    return BooleanClause.newBuilder().setQuery(query).setOccur(occur).build();
  }
}
