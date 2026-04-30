# Robust Vector vs Text Detection for Vector Field Indexing

## Problem

When a vector field has an embedding provider configured, the current code uses `value.trim().startsWith("[")` to decide whether a value is a JSON vector array or text to embed. This means any text that starts with `[` (e.g., `"[UPDATE] Product description"`) is incorrectly treated as a JSON array and fails to parse, instead of being sent to the embedding provider.

## Design

### Core Detection: `looksLikeVectorJson`

Replace the `startsWith("[")` check with a static method on `VectorFieldDef` that uses three layers of filtering (cheapest first):

1. **`startsWith("[")`** — instant reject for text that clearly isn't a vector
2. **Regex match** — catches `[UPDATE] foo`, `[1, 2, "three"]`, etc. without GSON overhead
3. **Delegate to existing parse methods** — if regex matches, treat as vector and let `parseVectorFieldToFloatArr` / `parseVectorFieldToByteArr` handle GSON parsing and dimension validation with proper error messages

```java
private static final Pattern VECTOR_JSON_PATTERN =
    Pattern.compile("^\\s*\\[\\s*-?\\d+(\\.\\d+)?([eE][+-]?\\d+)?\\s*(,\\s*-?\\d+(\\.\\d+)?([eE][+-]?\\d+)?\\s*)*]\\s*$");

static boolean looksLikeVectorJson(String value) {
    String trimmed = value.trim();
    return trimmed.startsWith("[") && VECTOR_JSON_PATTERN.matcher(trimmed).matches();
}
```

### Integration into `parseVectorField`

Both `FloatVectorFieldDef` and `ByteVectorFieldDef` follow the same flow:

```java
void parseVectorField(String value, Document document) {
    float[] floatArr = null;

    if (looksLikeVectorJson(value)) {
        // Matched regex — must be a valid vector, no fallback to embedding
        floatArr = parseVectorFieldToFloatArr(value);  // throws on wrong dimensions
    } else if (getEmbeddingProviderName() != null) {
        // Doesn't look like a vector — embed it
        EmbeddingProvider provider =
            EmbeddingCreator.getInstance().getProvider(getEmbeddingProviderName());
        if (provider == null) {
            throw new IllegalArgumentException(
                "Embedding provider not found: " + getEmbeddingProviderName());
        }
        floatArr = provider.embed(value);
        // dimension validation...
    }

    // docValues and searchable paths unchanged
}
```

Key: if the value matches the regex, it is committed to the vector parse path. A valid JSON array with wrong dimensions will throw a dimension error — it will NOT fall through to embedding.

### Behavior Matrix

| Input | Matches regex? | Has provider? | Result |
|---|---|---|---|
| `[1.0, 2.0, 3.0]` correct dims | yes | any | Parsed as vector |
| `[1.0, 2.0, 3.0]` wrong dims | yes | any | Dimension error |
| `[UPDATE] some text` | no | yes | Embedded |
| `[UPDATE] some text` | no | no | GSON parse fails (same as today) |
| `hello world` | no | yes | Embedded |
| `hello world` | no | no | GSON parse fails (same as today) |

### Code Placement

- `VECTOR_JSON_PATTERN` and `looksLikeVectorJson` live as `private static` / package-private `static` members on the parent `VectorFieldDef` class
- Both `FloatVectorFieldDef` and `ByteVectorFieldDef` (inner classes) use them
- No new files or abstractions

### Explicit Declaration (Future)

The [typed indexing values design](2026-04-30-typed-indexing-values-design.md) will provide an unambiguous path: when `FieldValue.valueType == BYTES`, the value is a vector with no detection needed. The string-based auto-detection described here continues to serve the `valueType == STRING` path.

## Test Plan

**Unit tests for `looksLikeVectorJson`:**
- `"[1.0, 2.0, 3.0]"` → true
- `"  [ 1.0 , 2.0 , 3.0 ]  "` → true (whitespace)
- `"[1, -2, 3]"` → true (integers, negatives)
- `"[1.5e10, -2.3E-4]"` → true (scientific notation)
- `"[UPDATE] some text"` → false
- `"[1, 2, \"three\"]"` → false (non-numeric)
- `"hello world"` → false
- `"[]"` → false (empty array)
- `"[1.0, 2.0"` → false (unclosed)
- `"1.0, 2.0]"` → false (no opening bracket)

**Integration tests for `parseVectorField` with embedding provider:**
- Text starting with `[` + provider → embedded successfully
- Valid JSON vector + correct dims + provider → parsed as vector, provider not called
- Valid JSON vector + wrong dims + provider → dimension error (not embedded)
- Plain text + provider → embedded successfully
- Plain text + no provider → parse error (same as today)

**Backwards compatibility:**
- All existing vector indexing tests pass unchanged
