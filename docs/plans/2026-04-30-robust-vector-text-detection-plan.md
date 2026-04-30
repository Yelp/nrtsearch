# Robust Vector vs Text Detection Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the brittle `startsWith("[")` check in vector field indexing with a robust regex + GSON + dimension validation approach, so that text starting with `[` is correctly embedded instead of crashing.

**Architecture:** Add a static `looksLikeVectorJson(String)` method and compiled regex to `VectorFieldDef`. Both `FloatVectorFieldDef.parseVectorField` and `ByteVectorFieldDef.parseVectorField` use it to decide the vector-vs-text path. If the regex matches, commit to the vector parse path (dimension errors are real errors). If it doesn't match and an embedding provider is configured, embed. No new files or abstractions.

**Tech Stack:** Java, JUnit, Mockito, Gson, `java.util.regex.Pattern`

**Design doc:** `docs/plans/2026-04-30-robust-vector-text-detection-design.md`

---

### Task 1: Add `looksLikeVectorJson` and regex to `VectorFieldDef`

**Files:**
- Modify: `src/main/java/com/yelp/nrtsearch/server/field/VectorFieldDef.java:73-102`

**Step 1: Add the regex pattern and method**

Add after line 102 (`private static final Gson GSON = ...`):

```java
import java.util.regex.Pattern;

// Matches a JSON array of numeric values: integers, decimals, scientific notation
// Examples: [1.0, -2, 3.5e10], [ 1 , 2 , 3 ]
private static final Pattern VECTOR_JSON_PATTERN =
    Pattern.compile(
        "^\\s*\\[\\s*-?\\d+(\\.\\d+)?([eE][+-]?\\d+)?\\s*(,\\s*-?\\d+(\\.\\d+)?([eE][+-]?\\d+)?\\s*)*]\\s*$");

@VisibleForTesting
static boolean looksLikeVectorJson(String value) {
  String trimmed = value.trim();
  return trimmed.startsWith("[") && VECTOR_JSON_PATTERN.matcher(trimmed).matches();
}
```

Note: `import java.util.regex.Pattern` needs to be added to the imports. `@VisibleForTesting` is already imported.

**Step 2: Verify compilation**

Run: `./gradlew :server:compileJava`
Expected: BUILD SUCCESSFUL

**Step 3: Commit**

```bash
git add src/main/java/com/yelp/nrtsearch/server/field/VectorFieldDef.java
git commit -m "feat: add looksLikeVectorJson regex method to VectorFieldDef"
```

---

### Task 2: Write unit tests for `looksLikeVectorJson`

**Files:**
- Modify: `src/test/java/com/yelp/nrtsearch/server/field/VectorFieldDefEmbeddingTest.java`

**Step 1: Add unit tests for the static method**

Add tests to `VectorFieldDefEmbeddingTest` (they don't need field instances, just call the static method):

```java
import com.yelp.nrtsearch.server.field.VectorFieldDef;

// --- looksLikeVectorJson unit tests ---

@Test
public void testLooksLikeVectorJson_validArray() {
  assertTrue(VectorFieldDef.looksLikeVectorJson("[1.0, 2.0, 3.0]"));
}

@Test
public void testLooksLikeVectorJson_integers() {
  assertTrue(VectorFieldDef.looksLikeVectorJson("[1, -2, 3]"));
}

@Test
public void testLooksLikeVectorJson_whitespace() {
  assertTrue(VectorFieldDef.looksLikeVectorJson("  [ 1.0 , 2.0 , 3.0 ]  "));
}

@Test
public void testLooksLikeVectorJson_scientificNotation() {
  assertTrue(VectorFieldDef.looksLikeVectorJson("[1.5e10, -2.3E-4]"));
}

@Test
public void testLooksLikeVectorJson_singleElement() {
  assertTrue(VectorFieldDef.looksLikeVectorJson("[42]"));
}

@Test
public void testLooksLikeVectorJson_textStartingWithBracket() {
  assertFalse(VectorFieldDef.looksLikeVectorJson("[UPDATE] some text"));
}

@Test
public void testLooksLikeVectorJson_nonNumericElement() {
  assertFalse(VectorFieldDef.looksLikeVectorJson("[1, 2, \"three\"]"));
}

@Test
public void testLooksLikeVectorJson_plainText() {
  assertFalse(VectorFieldDef.looksLikeVectorJson("hello world"));
}

@Test
public void testLooksLikeVectorJson_emptyArray() {
  assertFalse(VectorFieldDef.looksLikeVectorJson("[]"));
}

@Test
public void testLooksLikeVectorJson_unclosedBracket() {
  assertFalse(VectorFieldDef.looksLikeVectorJson("[1.0, 2.0"));
}

@Test
public void testLooksLikeVectorJson_noOpeningBracket() {
  assertFalse(VectorFieldDef.looksLikeVectorJson("1.0, 2.0]"));
}

@Test
public void testLooksLikeVectorJson_nestedArray() {
  assertFalse(VectorFieldDef.looksLikeVectorJson("[[1, 2], [3, 4]]"));
}

@Test
public void testLooksLikeVectorJson_emptyString() {
  assertFalse(VectorFieldDef.looksLikeVectorJson(""));
}
```

**Step 2: Run tests**

Run: `./gradlew :server:test --tests "com.yelp.nrtsearch.server.field.VectorFieldDefEmbeddingTest"`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add src/test/java/com/yelp/nrtsearch/server/field/VectorFieldDefEmbeddingTest.java
git commit -m "test: add unit tests for looksLikeVectorJson"
```

---

### Task 3: Update `FloatVectorFieldDef.parseVectorField` to use `looksLikeVectorJson`

**Files:**
- Modify: `src/main/java/com/yelp/nrtsearch/server/field/VectorFieldDef.java:483-523`

**Step 1: Rewrite the detection logic**

Replace lines 483-523 of `FloatVectorFieldDef.parseVectorField` with:

```java
@Override
void parseVectorField(String value, Document document) {
  float[] floatArr = null;

  if (looksLikeVectorJson(value)) {
    // Regex matched — parse as vector. Dimension errors are real errors.
    floatArr = parseVectorFieldToFloatArr(value);
  } else if (getEmbeddingProviderName() != null) {
    // Doesn't look like a vector — embed the text
    EmbeddingProvider provider =
        EmbeddingCreator.getInstance().getProvider(getEmbeddingProviderName());
    if (provider == null) {
      throw new IllegalArgumentException(
          "Embedding provider not found: " + getEmbeddingProviderName());
    }
    floatArr = provider.embed(value);
    if (floatArr.length != getVectorDimensions()) {
      throw new IllegalArgumentException(
          "Embedding provider returned vector of size "
              + floatArr.length
              + " but field expects "
              + getVectorDimensions());
    }
  }

  if (hasDocValues() && docValuesType == DocValuesType.BINARY) {
    if (floatArr == null) {
      floatArr = parseVectorFieldToFloatArr(value);
    }
    byte[] floatBytes = convertFloatArrToBytes(floatArr);
    document.add(new BinaryDocValuesField(getName(), new BytesRef(floatBytes)));
  }
  if (isSearchable()) {
    if (floatArr == null) {
      floatArr = parseVectorFieldToFloatArr(value);
    }
    float magnitude2 = validateVectorForSearch(floatArr);
    if (magnitudeField != null) {
      float magnitude = (float) Math.sqrt(magnitude2);
      normalizeVector(floatArr, magnitude);
      magnitudeField.parseDocumentField(document, List.of(String.valueOf(magnitude)), null);
    }
    document.add(new KnnFloatVectorField(getName(), floatArr, similarityFunction));
  }
}
```

**Step 2: Run existing tests**

Run: `./gradlew :server:test --tests "com.yelp.nrtsearch.server.field.VectorFieldDefEmbeddingTest"` and `./gradlew :server:test --tests "com.yelp.nrtsearch.server.field.VectorFieldDefTest"`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add src/main/java/com/yelp/nrtsearch/server/field/VectorFieldDef.java
git commit -m "refactor: use looksLikeVectorJson in FloatVectorFieldDef.parseVectorField"
```

---

### Task 4: Update `ByteVectorFieldDef.parseVectorField` to use `looksLikeVectorJson`

**Files:**
- Modify: `src/main/java/com/yelp/nrtsearch/server/field/VectorFieldDef.java:733-768`

**Step 1: Rewrite the detection logic**

Replace lines 733-768 of `ByteVectorFieldDef.parseVectorField` with the same pattern:

```java
@Override
void parseVectorField(String value, Document document) {
  byte[] byteArr = null;

  if (looksLikeVectorJson(value)) {
    // Regex matched — parse as vector. Dimension errors are real errors.
    byteArr = parseVectorFieldToByteArr(value);
  } else if (getEmbeddingProviderName() != null) {
    // Doesn't look like a vector — embed the text
    EmbeddingProvider provider =
        EmbeddingCreator.getInstance().getProvider(getEmbeddingProviderName());
    if (provider == null) {
      throw new IllegalArgumentException(
          "Embedding provider not found: " + getEmbeddingProviderName());
    }
    byteArr = provider.embedBytes(value);
    if (byteArr.length != getVectorDimensions()) {
      throw new IllegalArgumentException(
          "Embedding provider returned vector of size "
              + byteArr.length
              + " but field expects "
              + getVectorDimensions());
    }
  }

  if (hasDocValues() && docValuesType == DocValuesType.BINARY) {
    if (byteArr == null) {
      byteArr = parseVectorFieldToByteArr(value);
    }
    document.add(new BinaryDocValuesField(getName(), new BytesRef(byteArr)));
  }
  if (isSearchable()) {
    if (byteArr == null) {
      byteArr = parseVectorFieldToByteArr(value);
    }
    validateVectorForSearch(byteArr);
    document.add(new KnnByteVectorField(getName(), byteArr, similarityFunction));
  }
}
```

**Step 2: Run all vector field tests**

Run: `./gradlew :server:test --tests "com.yelp.nrtsearch.server.field.VectorFieldDef*"`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add src/main/java/com/yelp/nrtsearch/server/field/VectorFieldDef.java
git commit -m "refactor: use looksLikeVectorJson in ByteVectorFieldDef.parseVectorField"
```

---

### Task 5: Add integration tests for text-starting-with-bracket embedding

**Files:**
- Modify: `src/test/java/com/yelp/nrtsearch/server/field/VectorFieldDefEmbeddingTest.java`

**Step 1: Add the new integration tests**

```java
@Test
public void testParseVectorFieldWithBracketTextAndProvider() {
  // Text starting with '[' should be embedded, not parsed as JSON
  FloatVectorFieldDef fieldDef = createFieldDefWithEmbedding("test-provider");
  Document document = new Document();

  fieldDef.parseVectorField("[UPDATE] Product description for embedding", document);

  assertEquals(1, document.getFields().size());
  IndexableField field = document.getFields().get(0);
  assertEquals("test_vector", field.name());
  assertNotNull(field.binaryValue());
}

@Test(expected = IllegalArgumentException.class)
public void testParseVectorFieldWrongDimensionsWithProviderStillThrows() {
  // A valid JSON vector with wrong dimensions should throw, NOT fall through to embedding
  FloatVectorFieldDef fieldDef = createFieldDefWithEmbeddingAndDimensions("test-provider", 5);
  Document document = new Document();

  fieldDef.parseVectorField("[1.0, 2.0, 3.0]", document);
}
```

**Step 2: Update the expected exception for text-without-provider test**

The existing test `testParseVectorFieldWithTextNoProviderFails` (line 106) expects `JsonSyntaxException`. After the change, plain text without a provider still falls through to `parseVectorFieldToFloatArr` which calls GSON, so this should still throw `JsonSyntaxException`. Verify this test still passes as-is.

**Step 3: Run tests**

Run: `./gradlew :server:test --tests "com.yelp.nrtsearch.server.field.VectorFieldDefEmbeddingTest"`
Expected: All tests PASS

**Step 4: Commit**

```bash
git add src/test/java/com/yelp/nrtsearch/server/field/VectorFieldDefEmbeddingTest.java
git commit -m "test: add integration tests for bracket-text embedding and wrong-dimension errors"
```

---

### Task 6: Update proto documentation comment

**Files:**
- Modify: `clientlib/src/main/proto/yelp/nrtsearch/luceneserver.proto:679-683`

**Step 1: Update the embeddingProvider field comment**

Replace the comment on the `embeddingProvider` field (line 679-683):

```protobuf
    // Name of a configured embedding provider. When set on a VECTOR field, text strings sent
    // during indexing are automatically converted to vectors using this provider.
    // For float vector fields, the provider's embed() method is used.
    // For byte vector fields, the provider's embedBytes() method is used.
    // Values that look like JSON numeric arrays (e.g., "[1.0, 2.0, 3.0]") are parsed as vectors
    // directly. All other values, including text starting with '[', are sent to the embedding
    // provider. If a JSON numeric array has the wrong number of dimensions, a dimension error
    // is thrown (it will NOT fall through to embedding).
```

**Step 2: Commit**

```bash
git add clientlib/src/main/proto/yelp/nrtsearch/luceneserver.proto
git commit -m "docs: update embeddingProvider proto comment for robust vector detection"
```

---

### Task 7: Run full test suite and verify backwards compatibility

**Step 1: Run all vector-related tests**

Run: `./gradlew :server:test --tests "com.yelp.nrtsearch.server.field.VectorFieldDef*"`
Expected: All tests PASS

**Step 2: Run full server test suite**

Run: `./gradlew :server:test`
Expected: BUILD SUCCESSFUL with no new failures

**Step 3: Commit (only if any fixups were needed)**