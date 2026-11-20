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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates warming queries from the templates of a few real queries.
 *
 * <p>A template is a real request with every value replaced by a slot. The structure is kept
 * exactly: query tree, scripts, sort, nested paths, collectors, top hits and retrieve fields. This
 * is the part of a client's request that a generator working from the index schema can't know, and
 * replaying it is what warms the same code paths, scripts and cached filters as live traffic.
 *
 * <p>Per template, each slot is classified from the seed queries that share the template:
 *
 * <ul>
 *   <li>constant: the same value in at least 90% of them (script params, constant filters)
 *   <li>field: a free value of an index field (terms, term sets, geo points, text), filled from the
 *       documents that the seed queries return. Using returned documents rather than random ones
 *       keeps values where traffic searches: live documents, populated areas.
 *   <li>linked: equal to another slot in most queries (a script param that repeats the geo centre),
 *       copied from that slot
 *   <li>donor: anything else (range bounds, radius, free params, low cardinality fields such as
 *       flags), taken from a seed query
 * </ul>
 *
 * Slots that are linked or read the same field form one value group, filled from one document, so
 * values stay consistent within a query. Use {@link #fields()} to get the fields to retrieve for
 * the value pool, {@link #addPoolHits(SearchResponse)} to add documents, then {@link
 * #generate(int)}.
 */
public class TemplateWarmingQueryGenerator {
  private static final Logger logger = LoggerFactory.getLogger(TemplateWarmingQueryGenerator.class);

  static final String SLOT_PREFIX = "§";
  private static final Set<String> SKIP_KEYS = Set.of("indexName", "timeoutSec", "startHit");
  // structural strings, part of the template rather than values
  private static final Set<String> STRUCT_KEYS =
      Set.of(
          "field",
          "fieldName",
          "path",
          "occur",
          "scoreMode",
          "lang",
          "source",
          "name",
          "op",
          "operator",
          "sortedFields",
          "retrieveFields",
          "type",
          "dateTimeFormat");
  // values that are not field values even when inside a field's query
  private static final Set<String> DONOR_LEAVES =
      Set.of(
          "lower",
          "upper",
          "lowerValue",
          "upperValue",
          "from",
          "to",
          "gte",
          "lte",
          "gt",
          "lt",
          "radius",
          "distance",
          "boost",
          "minimumNumberShouldMatch",
          "topHits",
          "size",
          "k",
          "numCandidates",
          "slop",
          "maxExpansions",
          "fuzziness");
  static final double CONST_SHARE = 0.9;
  static final double LINK_SHARE = 0.8;
  static final int MIN_INSTANCES = 3;
  static final int MAX_DONORS = 200;
  // fields with at most this many distinct real values keep their real values
  static final int LOW_CARDINALITY = 10;
  // documents kept from each seed query's hits for the value pool
  static final int POOL_HITS_PER_QUERY = 20;

  private final List<Template> templates = new ArrayList<>();
  private final String indexName;
  private final int numSeeds;
  private final Random random;
  // per document: field -> values
  private final List<Map<String, List<JsonElement>>> poolDocs = new ArrayList<>();
  private final Map<String, List<List<JsonElement>>> poolByField = new HashMap<>();

  /**
   * @param seeds real queries to learn templates from
   * @param random random source
   */
  public TemplateWarmingQueryGenerator(List<SearchRequest> seeds, Random random) {
    this.random = random;
    this.numSeeds = seeds.size();
    this.indexName = seeds.isEmpty() ? "" : seeds.get(0).getIndexName();
    learn(seeds);
  }

  private TemplateWarmingQueryGenerator(
      String indexName, List<Template> templates, int numSeeds, Random random) {
    this.random = random;
    this.numSeeds = numSeeds;
    this.indexName = indexName;
    this.templates.addAll(templates);
    this.templates.sort((a, b) -> Double.compare(b.weight, a.weight));
  }

  public int getNumTemplates() {
    return templates.size();
  }

  // ------------------------------------------------------------------ template library

  static final int LIBRARY_VERSION = 1;

  /**
   * Template key of a request: its structure with values replaced, the same key the templates are
   * grouped by. Used to check whether live queries still match the library.
   */
  public static String templateKey(SearchRequest request) {
    try {
      JsonElement json = JsonParser.parseString(JsonFormat.printer().print(request));
      return templatize(json, "", null, new ArrayList<>()).toString();
    } catch (InvalidProtocolBufferException | RuntimeException e) {
      return null;
    }
  }

  /** Keys of all templates, see {@link #templateKey(SearchRequest)}. */
  public Set<String> templateKeys() {
    Set<String> keys = new HashSet<>();
    for (Template t : templates) {
      keys.add(t.skeleton.toString());
    }
    return keys;
  }

  /**
   * Merge newly learned templates into this library, as a new generator. Weights of this library's
   * templates are multiplied by decay, so templates that stop occurring in traffic fade out, and
   * templates whose weight falls below minWeight are dropped. A template in both keeps the newer
   * classification when it was learned from enough queries, and the donors of both, capped.
   */
  public TemplateWarmingQueryGenerator merge(
      TemplateWarmingQueryGenerator newer, double decay, double minWeight) {
    Map<String, Template> merged = new LinkedHashMap<>();
    for (Template t : templates) {
      t.weight *= decay;
      merged.put(t.skeleton.toString(), t);
    }
    for (Template n : newer.templates) {
      String key = n.skeleton.toString();
      Template old = merged.get(key);
      if (old == null) {
        merged.put(key, n);
        continue;
      }
      Template keep = n.count >= MIN_INSTANCES || n.count >= old.count ? n : old;
      Template other = keep == n ? old : n;
      keep.weight = old.weight + n.weight;
      // donors of the other side only if they have the same variable slots
      if (other.slots.keySet().equals(keep.slots.keySet())) {
        List<Map<String, JsonElement>> donors = new ArrayList<>(keep.donors);
        donors.addAll(other.donors);
        java.util.Collections.shuffle(donors, random);
        keep.donors.clear();
        keep.donors.addAll(donors.subList(0, Math.min(MAX_DONORS, donors.size())));
      }
      merged.put(key, keep);
    }
    List<Template> kept = new ArrayList<>();
    for (Template t : merged.values()) {
      if (t.weight >= minWeight && !t.donors.isEmpty()) {
        kept.add(t);
      }
    }
    String name = indexName.isEmpty() ? newer.indexName : indexName;
    return new TemplateWarmingQueryGenerator(name, kept, numSeeds + newer.numSeeds, random);
  }

  /** Serialize the templates as a library, see {@link #fromLibrary(String, Random)}. */
  public String toLibrary() {
    JsonObject root = new JsonObject();
    root.addProperty("version", LIBRARY_VERSION);
    root.addProperty("indexName", indexName);
    root.addProperty("seedQueries", numSeeds);
    JsonArray list = new JsonArray();
    for (Template t : templates) {
      JsonObject o = new JsonObject();
      o.add("skeleton", t.skeleton);
      o.addProperty("count", t.count);
      o.addProperty("weight", t.weight);
      JsonObject constants = new JsonObject();
      t.constants.forEach(constants::add);
      o.add("constants", constants);
      JsonObject slots = new JsonObject();
      t.slots.forEach(
          (id, m) -> {
            JsonObject s = new JsonObject();
            s.addProperty("path", m.path);
            if (m.ctx != null) {
              s.addProperty("ctx", m.ctx);
            }
            s.addProperty("field", m.field);
            slots.add(id, s);
          });
      o.add("slots", slots);
      JsonObject links = new JsonObject();
      t.links.forEach((id, others) -> links.add(id, stringArray(others)));
      o.add("links", links);
      JsonArray groups = new JsonArray();
      t.groups.forEach(g -> groups.add(stringArray(g)));
      o.add("groups", groups);
      JsonArray donors = new JsonArray();
      for (Map<String, JsonElement> d : t.donors) {
        JsonObject donor = new JsonObject();
        d.forEach(donor::add);
        donors.add(donor);
      }
      o.add("donors", donors);
      list.add(o);
    }
    root.add("templates", list);
    return root.toString();
  }

  /** Load a library written by {@link #toLibrary()}. */
  public static TemplateWarmingQueryGenerator fromLibrary(String library, Random random) {
    JsonObject root = JsonParser.parseString(library).getAsJsonObject();
    int version = root.get("version").getAsInt();
    if (version != LIBRARY_VERSION) {
      throw new IllegalArgumentException("Unsupported template library version: " + version);
    }
    List<Template> templates = new ArrayList<>();
    for (JsonElement e : root.getAsJsonArray("templates")) {
      JsonObject o = e.getAsJsonObject();
      Template t = new Template();
      t.skeleton = o.get("skeleton");
      t.count = o.get("count").getAsInt();
      t.weight = o.get("weight").getAsDouble();
      o.getAsJsonObject("constants")
          .entrySet()
          .forEach(c -> t.constants.put(c.getKey(), c.getValue()));
      for (Map.Entry<String, JsonElement> s : o.getAsJsonObject("slots").entrySet()) {
        JsonObject m = s.getValue().getAsJsonObject();
        t.slots.put(
            s.getKey(),
            new SlotMeta(
                m.get("path").getAsString(),
                m.has("ctx") ? m.get("ctx").getAsString() : null,
                m.get("field").getAsBoolean()));
      }
      for (Map.Entry<String, JsonElement> l : o.getAsJsonObject("links").entrySet()) {
        t.links.put(l.getKey(), strings(l.getValue().getAsJsonArray()));
      }
      for (JsonElement g : o.getAsJsonArray("groups")) {
        t.groups.add(strings(g.getAsJsonArray()));
      }
      for (JsonElement d : o.getAsJsonArray("donors")) {
        Map<String, JsonElement> donor = new HashMap<>();
        d.getAsJsonObject().entrySet().forEach(x -> donor.put(x.getKey(), x.getValue()));
        t.donors.add(donor);
      }
      templates.add(t);
    }
    return new TemplateWarmingQueryGenerator(
        root.get("indexName").getAsString(), templates, root.get("seedQueries").getAsInt(), random);
  }

  private static JsonArray stringArray(List<String> values) {
    JsonArray a = new JsonArray();
    values.forEach(a::add);
    return a;
  }

  private static List<String> strings(JsonArray a) {
    List<String> out = new ArrayList<>();
    a.forEach(x -> out.add(x.getAsString()));
    return out;
  }

  /** Fields to retrieve from the seed queries' hits for the value pool. */
  public Set<String> fields() {
    Set<String> fields = new TreeSet<>();
    for (Template t : templates) {
      for (SlotMeta m : t.slots.values()) {
        if (m.field) {
          fields.add(m.ctx);
        }
      }
    }
    return fields;
  }

  /** Add the documents of a seed query's response to the value pool. */
  public void addPoolHits(SearchResponse response) {
    int kept = 0;
    for (SearchResponse.Hit hit : response.getHitsList()) {
      if (kept++ >= POOL_HITS_PER_QUERY) {
        break;
      }
      Map<String, List<JsonElement>> doc = new HashMap<>();
      for (Map.Entry<String, SearchResponse.Hit.CompositeFieldValue> e :
          hit.getFieldsMap().entrySet()) {
        List<JsonElement> values = new ArrayList<>();
        for (SearchResponse.Hit.FieldValue fv : e.getValue().getFieldValueList()) {
          JsonElement v = fieldValue(fv);
          if (v != null) {
            values.add(v);
          }
        }
        if (!values.isEmpty()) {
          doc.put(e.getKey(), values);
          poolByField.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).add(values);
        }
      }
      if (!doc.isEmpty()) {
        poolDocs.add(doc);
      }
    }
  }

  public int getPoolSize() {
    return poolDocs.size();
  }

  private static JsonElement fieldValue(SearchResponse.Hit.FieldValue fv) {
    try {
      JsonObject o = JsonParser.parseString(JsonFormat.printer().print(fv)).getAsJsonObject();
      if (o.size() != 1) {
        return null;
      }
      return o.entrySet().iterator().next().getValue();
    } catch (InvalidProtocolBufferException | RuntimeException e) {
      return null;
    }
  }

  // ------------------------------------------------------------------ learn

  private record Slot(String id, String path, String ctx, JsonElement value) {}

  private static final class SlotMeta {
    final String path;
    final String ctx;
    final boolean field;

    SlotMeta(String path, String ctx, boolean field) {
      this.path = path;
      this.ctx = ctx;
      this.field = field;
    }
  }

  private static final class Template {
    JsonElement skeleton;
    // seed queries the classification was learned from
    int count;
    // how often the template occurs: its count, decayed by later library merges
    double weight;
    final Map<String, JsonElement> constants = new LinkedHashMap<>();
    // variable slots only
    final Map<String, SlotMeta> slots = new LinkedHashMap<>();
    final Map<String, List<String>> links = new HashMap<>();
    final List<List<String>> groups = new ArrayList<>();
    final List<Map<String, JsonElement>> donors = new ArrayList<>();
  }

  private void learn(List<SearchRequest> seeds) {
    Map<String, List<List<Slot>>> slotsByTemplate = new LinkedHashMap<>();
    Map<String, JsonElement> skeletons = new HashMap<>();
    for (SearchRequest seed : seeds) {
      JsonElement json;
      try {
        json = JsonParser.parseString(JsonFormat.printer().print(seed));
      } catch (InvalidProtocolBufferException | RuntimeException e) {
        continue;
      }
      List<Slot> slots = new ArrayList<>();
      JsonElement skeleton = templatize(json, "", null, slots);
      String key = skeleton.toString();
      skeletons.putIfAbsent(key, skeleton);
      slotsByTemplate.computeIfAbsent(key, k -> new ArrayList<>()).add(slots);
    }
    for (Map.Entry<String, List<List<Slot>>> e : slotsByTemplate.entrySet()) {
      templates.add(buildTemplate(skeletons.get(e.getKey()), e.getValue()));
    }
    templates.sort((a, b) -> Double.compare(b.weight, a.weight));
  }

  private Template buildTemplate(JsonElement skeleton, List<List<Slot>> instances) {
    Template t = new Template();
    t.skeleton = skeleton;
    t.count = instances.size();
    t.weight = instances.size();
    List<Slot> first = instances.get(0);
    int n = instances.size();
    List<String> variable = new ArrayList<>();
    Map<String, List<String>> normValues = new HashMap<>();
    for (int i = 0; i < first.size(); ++i) {
      Slot s = first.get(i);
      List<String> values = new ArrayList<>(n);
      Map<String, Integer> counts = new HashMap<>();
      int top = 0;
      for (List<Slot> inst : instances) {
        String v = norm(inst.get(i).value());
        values.add(v);
        top = Math.max(top, counts.merge(v, 1, Integer::sum));
      }
      normValues.put(s.id(), values);
      // with few instances, constants can't be told apart from coincidence, so everything varies
      if (n >= MIN_INSTANCES && top >= CONST_SHARE * n) {
        t.constants.put(s.id(), s.value());
      } else {
        variable.add(s.id());
        boolean field =
            s.ctx() != null
                && !s.ctx().startsWith("param:")
                && !DONOR_LEAVES.contains(leaf(s.path()));
        t.slots.put(s.id(), new SlotMeta(s.path(), s.ctx(), field));
      }
    }
    // union find over variable slots: equal in most instances, or reading the same field
    Map<String, String> parent = new HashMap<>();
    for (String s : variable) {
      parent.put(s, s);
    }
    if (n >= MIN_INSTANCES) {
      for (int i = 0; i < variable.size(); ++i) {
        for (int j = i + 1; j < variable.size(); ++j) {
          String a = variable.get(i), b = variable.get(j);
          List<String> va = normValues.get(a), vb = normValues.get(b);
          int eq = 0;
          for (int k = 0; k < n; ++k) {
            if (va.get(k).equals(vb.get(k))) {
              eq++;
            }
          }
          if (eq >= LINK_SHARE * n) {
            union(parent, a, b);
            t.links.computeIfAbsent(a, k -> new ArrayList<>()).add(b);
            t.links.computeIfAbsent(b, k -> new ArrayList<>()).add(a);
          }
        }
      }
    }
    Map<String, String> firstOfField = new HashMap<>();
    for (String s : variable) {
      SlotMeta m = t.slots.get(s);
      if (m.field) {
        String other = firstOfField.putIfAbsent(m.ctx, s);
        if (other != null) {
          union(parent, s, other);
        }
      }
    }
    Map<String, List<String>> groups = new LinkedHashMap<>();
    for (String s : variable) {
      groups.computeIfAbsent(find(parent, s), k -> new ArrayList<>()).add(s);
    }
    t.groups.addAll(groups.values());
    List<Integer> order = new ArrayList<>();
    for (int i = 0; i < n; ++i) {
      order.add(i);
    }
    java.util.Collections.shuffle(order, new Random(1));
    for (int idx : order.subList(0, Math.min(n, MAX_DONORS))) {
      Map<String, JsonElement> donor = new HashMap<>();
      for (Slot s : instances.get(idx)) {
        if (t.slots.containsKey(s.id())) {
          donor.put(s.id(), s.value());
        }
      }
      t.donors.add(donor);
    }
    return t;
  }

  private static String find(Map<String, String> parent, String x) {
    while (!parent.get(x).equals(x)) {
      parent.put(x, parent.get(parent.get(x)));
      x = parent.get(x);
    }
    return x;
  }

  private static void union(Map<String, String> parent, String a, String b) {
    parent.put(find(parent, a), find(parent, b));
  }

  /** Replace values by slot ids, keys in sorted order; record each slot's path and field. */
  private static JsonElement templatize(
      JsonElement node, String path, String ctx, List<Slot> slots) {
    if (node.isJsonObject()) {
      JsonObject o = node.getAsJsonObject();
      String field = ctx;
      for (String fk : List.of("field", "fieldName")) {
        if (o.has(fk) && o.get(fk).isJsonPrimitive()) {
          field = o.get(fk).getAsString();
          break;
        }
      }
      JsonObject out = new JsonObject();
      for (String k : new TreeSet<>(o.keySet())) {
        JsonElement v = o.get(k);
        if (SKIP_KEYS.contains(k)) {
          continue;
        }
        if (STRUCT_KEYS.contains(k)) {
          out.add(k, v.deepCopy());
        } else if (k.equals("params") && v.isJsonObject()) {
          JsonObject params = new JsonObject();
          for (String pk : new TreeSet<>(v.getAsJsonObject().keySet())) {
            params.add(
                pk,
                templatize(
                    v.getAsJsonObject().get(pk), path + "/params/" + pk, "param:" + pk, slots));
          }
          out.add(k, params);
        } else {
          out.add(k, templatize(v, path + "/" + k, field, slots));
        }
      }
      return out;
    }
    if (node.isJsonArray()) {
      JsonArray a = node.getAsJsonArray();
      if (!a.isEmpty() && (allPrimitive(a) || allWrappers(a))) {
        return addSlot(path, ctx, node, slots);
      }
      JsonArray out = new JsonArray();
      for (int i = 0; i < a.size(); ++i) {
        out.add(templatize(a.get(i), path + "[" + i + "]", ctx, slots));
      }
      return out;
    }
    return addSlot(path, ctx, node, slots);
  }

  private static JsonElement addSlot(String path, String ctx, JsonElement value, List<Slot> slots) {
    String id = SLOT_PREFIX + slots.size();
    slots.add(new Slot(id, path, ctx, value));
    return new JsonPrimitive(id);
  }

  private static boolean allPrimitive(JsonArray a) {
    for (JsonElement e : a) {
      if (!e.isJsonPrimitive()) {
        return false;
      }
    }
    return true;
  }

  private static boolean isWrapper(JsonElement e) {
    return e.isJsonObject()
        && e.getAsJsonObject().size() == 1
        && e.getAsJsonObject().entrySet().iterator().next().getValue().isJsonPrimitive();
  }

  private static boolean allWrappers(JsonArray a) {
    for (JsonElement e : a) {
      if (!isWrapper(e)) {
        return false;
      }
    }
    return true;
  }

  private static String leaf(String path) {
    String last = path.substring(path.lastIndexOf('/') + 1);
    int bracket = last.indexOf('[');
    return bracket >= 0 ? last.substring(0, bracket) : last;
  }

  /** Comparable form of a value: arrays sorted, wrappers unwrapped, numbers rounded. */
  static String norm(JsonElement v) {
    if (v == null || v.isJsonNull()) {
      return "null";
    }
    if (v.isJsonArray()) {
      List<String> parts = new ArrayList<>();
      for (JsonElement e : v.getAsJsonArray()) {
        parts.add(norm(e));
      }
      parts.sort(null);
      return parts.toString();
    }
    if (isWrapper(v)) {
      return norm(v.getAsJsonObject().entrySet().iterator().next().getValue());
    }
    if (v.isJsonPrimitive()) {
      JsonPrimitive p = v.getAsJsonPrimitive();
      if (p.isBoolean()) {
        return p.getAsString();
      }
      try {
        return String.format("%.5f", Double.parseDouble(p.getAsString()));
      } catch (NumberFormatException e) {
        return p.getAsString();
      }
    }
    return v.toString();
  }

  // ------------------------------------------------------------------ generate

  /**
   * Generate up to numQueries distinct queries. Templates are picked by how often they occur in the
   * seeds. Requests that fail to parse back are skipped.
   */
  public List<SearchRequest> generate(int numQueries) {
    List<SearchRequest> out = new ArrayList<>();
    if (templates.isEmpty() || numQueries <= 0) {
      return out;
    }
    Set<String> lowCardinality = lowCardinalityFields();
    Set<String> emitted = new HashSet<>();
    int attempts = 0;
    int parseFailures = 0;
    while (out.size() < numQueries && attempts < numQueries * 30) {
      attempts++;
      Template t = pickTemplate();
      Map<String, JsonElement> values = new HashMap<>(t.constants);
      for (List<String> group : t.groups) {
        fillGroup(t, group, values, lowCardinality);
      }
      JsonElement q = fill(t.skeleton, values);
      String key = q.toString();
      if (!emitted.add(key)) {
        continue;
      }
      JsonObject request = q.getAsJsonObject();
      request.addProperty("indexName", indexName);
      SearchRequest.Builder builder = SearchRequest.newBuilder();
      try {
        JsonFormat.parser().ignoringUnknownFields().merge(request.toString(), builder);
      } catch (InvalidProtocolBufferException | RuntimeException e) {
        parseFailures++;
        continue;
      }
      out.add(builder.build());
    }
    logger.info(
        "Generated {} template warming queries for index: {} from {} templates of {} seed queries,"
            + " value pool {} documents, low cardinality fields kept real: {}, unparsable: {}",
        out.size(),
        indexName,
        templates.size(),
        numSeeds,
        poolDocs.size(),
        lowCardinality,
        parseFailures);
    return out;
  }

  private Template pickTemplate() {
    double total = 0;
    for (Template t : templates) {
      total += t.weight;
    }
    double r = random.nextDouble() * total;
    for (Template t : templates) {
      r -= t.weight;
      if (r < 0) {
        return t;
      }
    }
    return templates.get(templates.size() - 1);
  }

  /** Fields whose seed values have few distinct values, such as flags, keep their real values. */
  private Set<String> lowCardinalityFields() {
    Map<String, Set<String>> distinct = new TreeMap<>();
    for (Template t : templates) {
      for (Map.Entry<String, SlotMeta> e : t.slots.entrySet()) {
        if (!e.getValue().field) {
          continue;
        }
        Set<String> values = distinct.computeIfAbsent(e.getValue().ctx, k -> new HashSet<>());
        for (Map<String, JsonElement> donor : t.donors) {
          JsonElement v = donor.get(e.getKey());
          if (v != null && v.isJsonArray()) {
            for (JsonElement x : v.getAsJsonArray()) {
              values.add(norm(x));
            }
          } else {
            values.add(norm(v));
          }
        }
      }
    }
    Set<String> low = new TreeSet<>();
    distinct.forEach(
        (f, vs) -> {
          if (vs.size() <= LOW_CARDINALITY) {
            low.add(f);
          }
        });
    return low;
  }

  private void fillGroup(
      Template t, List<String> group, Map<String, JsonElement> values, Set<String> lowCardinality) {
    Map<String, JsonElement> donor = t.donors.get(random.nextInt(t.donors.size()));
    List<String> fieldSlots = new ArrayList<>();
    Set<String> wanted = new HashSet<>();
    for (String s : group) {
      SlotMeta m = t.slots.get(s);
      if (m.field && !lowCardinality.contains(m.ctx) && poolByField.containsKey(m.ctx)) {
        fieldSlots.add(s);
        wanted.add(m.ctx);
      }
    }
    Map<String, List<JsonElement>> doc = null;
    if (!fieldSlots.isEmpty() && !poolDocs.isEmpty()) {
      for (int i = 0; i < 20; ++i) {
        Map<String, List<JsonElement>> d = poolDocs.get(random.nextInt(poolDocs.size()));
        if (d.keySet().containsAll(wanted)) {
          doc = d;
          break;
        }
      }
    }
    Map<String, JsonElement> filled = new HashMap<>();
    for (String s : fieldSlots) {
      SlotMeta m = t.slots.get(s);
      JsonElement like = donor.get(s);
      if (like == null) {
        continue;
      }
      List<JsonElement> docValues =
          doc != null && doc.containsKey(m.ctx) ? doc.get(m.ctx) : randomValues(m.ctx);
      if (like.isJsonArray()) {
        JsonArray likeArray = like.getAsJsonArray();
        if (likeArray.isEmpty() || isWrapper(likeArray.get(0))) {
          continue;
        }
        // term set from the same document, up to the seed's length
        List<JsonElement> shuffled = new ArrayList<>(docValues);
        java.util.Collections.shuffle(shuffled, random);
        JsonArray terms = new JsonArray();
        Set<String> seen = new HashSet<>();
        for (JsonElement v : shuffled) {
          JsonElement c = coerce(v, likeArray.get(0));
          if (c != null && seen.add(c.toString())) {
            terms.add(c);
          }
          if (terms.size() == likeArray.size()) {
            break;
          }
        }
        if (!terms.isEmpty()) {
          filled.put(s, terms);
        }
      } else {
        JsonElement c = fromValues(docValues, like, m.path);
        if (c != null) {
          filled.put(s, c);
        }
      }
    }
    for (String s : group) {
      if (filled.containsKey(s)) {
        values.put(s, filled.get(s));
        continue;
      }
      JsonElement like = donor.get(s);
      JsonElement linkedValue = null;
      for (String other : t.links.getOrDefault(s, List.of())) {
        JsonElement f = filled.get(other);
        if (f != null && !f.isJsonArray() && like != null) {
          linkedValue = coerce(f, like);
          if (linkedValue != null) {
            break;
          }
        }
      }
      values.put(s, linkedValue != null ? linkedValue : (like == null ? JsonNull.INSTANCE : like));
    }
  }

  private List<JsonElement> randomValues(String field) {
    List<List<JsonElement>> all = poolByField.get(field);
    return all.get(random.nextInt(all.size()));
  }

  private JsonElement fromValues(List<JsonElement> values, JsonElement like, String path) {
    if (values.isEmpty()) {
      return null;
    }
    JsonElement v = values.get(random.nextInt(values.size()));
    if (v.isJsonObject() && v.getAsJsonObject().has("latitude")) {
      String last = leaf(path).toLowerCase();
      if (last.equals("latitude") || last.equals("lat")) {
        return coerce(v.getAsJsonObject().get("latitude"), like);
      }
      if (last.equals("longitude") || last.equals("lon") || last.equals("lng")) {
        return coerce(v.getAsJsonObject().get("longitude"), like);
      }
      return null;
    }
    // text: a span of as many words as the seed query had
    if (like.isJsonPrimitive()
        && like.getAsJsonPrimitive().isString()
        && like.getAsString().trim().contains(" ")
        && v.isJsonPrimitive()) {
      String[] words = v.getAsString().split("\\s+");
      int k = like.getAsString().trim().split("\\s+").length;
      if (words.length >= k) {
        int start = random.nextInt(words.length - k + 1);
        return new JsonPrimitive(
            String.join(" ", java.util.Arrays.copyOfRange(words, start, start + k)));
      }
    }
    return coerce(v, like);
  }

  /** The new value in the type and wrapping of like, or null if it doesn't fit. */
  static JsonElement coerce(JsonElement value, JsonElement like) {
    if (value == null || like == null) {
      return null;
    }
    if (isWrapper(like)) {
      Map.Entry<String, JsonElement> e = like.getAsJsonObject().entrySet().iterator().next();
      JsonElement inner = coerce(value, e.getValue());
      if (inner == null) {
        return null;
      }
      JsonObject o = new JsonObject();
      o.add(e.getKey(), inner);
      return o;
    }
    if (!value.isJsonPrimitive() || !like.isJsonPrimitive()) {
      return null;
    }
    JsonPrimitive v = value.getAsJsonPrimitive();
    JsonPrimitive l = like.getAsJsonPrimitive();
    try {
      if (l.isBoolean()) {
        if (v.isBoolean()) {
          return v;
        }
        String s = v.getAsString();
        return s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")
            ? new JsonPrimitive(Boolean.parseBoolean(s))
            : null;
      }
      if (l.isNumber()) {
        String ls = l.getAsString();
        boolean integral = !(ls.contains(".") || ls.contains("e") || ls.contains("E"));
        double d = Double.parseDouble(v.getAsString());
        return integral ? new JsonPrimitive((long) d) : new JsonPrimitive(d);
      }
      // strings, including int64 values printed as strings
      if (v.isNumber() && !l.getAsString().contains(".")) {
        double d = v.getAsDouble();
        if (d == Math.rint(d)) {
          return new JsonPrimitive(Long.toString((long) d));
        }
      }
      return new JsonPrimitive(v.getAsString());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  static JsonElement fill(JsonElement node, Map<String, JsonElement> values) {
    if (node.isJsonObject()) {
      JsonObject out = new JsonObject();
      for (Map.Entry<String, JsonElement> e : node.getAsJsonObject().entrySet()) {
        out.add(e.getKey(), fill(e.getValue(), values));
      }
      return out;
    }
    if (node.isJsonArray()) {
      JsonArray out = new JsonArray();
      for (JsonElement e : node.getAsJsonArray()) {
        out.add(fill(e, values));
      }
      return out;
    }
    if (node.isJsonPrimitive() && node.getAsJsonPrimitive().isString()) {
      JsonElement v = values.get(node.getAsString());
      if (v != null) {
        return v.deepCopy();
      }
    }
    return node;
  }

  /** Visible for tests: number of slots of each kind over all templates. */
  Map<String, Integer> slotKinds() {
    Map<String, Integer> kinds = new TreeMap<>();
    for (Template t : templates) {
      kinds.merge("constant", t.constants.size(), Integer::sum);
      for (SlotMeta m : t.slots.values()) {
        kinds.merge(m.field ? "field" : "donor", 1, Integer::sum);
      }
    }
    return kinds;
  }
}
