# Skip Raw Vectors — Upgrade-Safe Codec Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hardcoded `Lucene104ScalarQuantizedVectorsReader` bypass in `VectorFieldDef` with a `FilterDirectory`-based approach that lets the real format's reader initialise normally, making the `skipRawVectorData` feature safe across Lucene codec version upgrades.

**Architecture:** Instead of constructing a version-specific reader chain by class name, we intercept at the `Directory` level. When a replica is missing `.vec`/`.vemf` files, a `FilterDirectory` wraps the segment's directory and synthesises minimal stub files — containing only the Lucene codec header/footer with zero field entries — for any raw flat vector file that is absent. The real `vectorsFormat.fieldsReader(state)` then runs normally: it opens the stub, reads zero field entries, and `Lucene104ScalarQuantizedVectorsReader.getFloatVectorValues()` falls back to its built-in dequantization path because `rawVectorsReader.getFloatVectorValues(field).size() == 0`. No version-specific reader class is referenced by name in nrtsearch code.

**Tech Stack:** Java 21, Lucene 10.4 (`org.apache.lucene.store.FilterDirectory`, `org.apache.lucene.codecs.CodecUtil`, `org.apache.lucene.store.ByteBuffersDataOutput`, `org.apache.lucene.store.ByteBuffersIndexInput`), JUnit 4, Mockito.

---

## Background for the implementer

### Why the current approach breaks on Lucene upgrades

`VectorFieldDef.createVectorsFormat()` currently contains this in `fieldsReader()` when `skipRawVectors=true`:

```java
return new Lucene99HnswVectorsReader(
    state,
    new Lucene104ScalarQuantizedVectorsReader(
        state,
        new NoOpFlatVectorsReader(),
        new Lucene104ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE)));
```

This hardcodes two class names: `Lucene104ScalarQuantizedVectorsReader` and `Lucene99HnswVectorsReader`. When Lucene is upgraded and these classes are renamed (e.g. to `Lucene105...`), the new classes will be used for writing but the old classes will attempt to read those files and fail with a codec-header mismatch.

### Why the FilterDirectory approach is upgrade-safe

`vectorsFormat.fieldsReader(state)` (the non-bypass path) always uses the correct reader for the codec version that wrote the segment — Lucene's `PerFieldKnnVectorsFormat` reads the codec name from the segment's field attributes and instantiates the right reader. We want to use this path even on replicas.

The only reason we can't use it today: `Lucene99FlatVectorsReader` (the innermost reader for raw flat vectors) calls `directory.openChecksumInput(vemfFileName)` in its constructor. If the `.vemf` file is missing, it throws `FileNotFoundException` before we can do anything useful.

The fix: wrap `state.directory` in a `FilterDirectory` that intercepts `openInput`/`openChecksumInput` for missing raw-vector files and returns a synthetic `IndexInput` containing a valid-but-empty codec payload. The reader then reads zero fields from it, `size()` returns 0, and the quantized fallback activates.

### What a valid empty .vemf file contains

From reverse-engineering `Lucene99FlatVectorsReader.readMetadata()`:

1. **Codec index header** — written by `CodecUtil.writeIndexHeader(out, "Lucene99FlatVectorsFormatMeta", VERSION_CURRENT=0, segmentId, segmentSuffix)`:
   - Magic int: `0x3fd76c17` (Lucene's `CODEC_MAGIC`)
   - Codec name as string: `"Lucene99FlatVectorsFormatMeta"` (length-prefixed)
   - Version int: `0`
   - Segment ID: 16 bytes (from `state.segmentInfo.getId()`)
   - Segment suffix as string: `state.segmentSuffix` (length-prefixed)
2. **Field entries**: none (zero fields, since we downloaded no raw vector data)
   - `readFields()` iterates `fieldInfos` and reads an entry for each field that has a vector — if the file contains zero fields the loop simply finds nothing for each field. The actual binary is: no bytes (empty loop body) — the field count is not written as a header, it's implied by iterating fieldInfos and looking each one up in the file.

   Actually `readFields` in `Lucene99FlatVectorsReader` reads entries **per-field** by seeking to known offsets. The loop in `readMetadata` iterates `fieldInfos` looking for `VectorEncoding` fields; for each, it tries to read a field entry from the meta file. If the meta file contains no field entries (only header+footer), this fails. We need to examine what `readFields` actually reads.

   Let's look more carefully: from the bytecode, `readFields` reads a `FieldEntry` per vector field. The safest approach is to write a complete valid header+footer with **no field entries between them** — this works because `readFields` will only attempt to read entries if there are fields in `fieldInfos` with KNN vectors. On a replica that skipped raw vector files, the stub's content after the header and before the footer is empty if there are no fields to declare. But if there ARE quantized-vector fields, `readFields` might expect entries.

   The critical insight: `Lucene99FlatVectorsReader` stores field data as offset+length pairs in the `.vemf` meta file. If a field entry is absent, `getFloatVectorValues(field)` returns null (field not found) or the stored entry has `size=0`. We need the reader to believe the file has zero vectors per field. The safest stub is a header + no field-count bytes + no field entries + footer — but this only works if `readFields` handles "field not in meta = size 0" gracefully.

   **Simpler alternative confirmed by bytecode**: `Lucene104ScalarQuantizedVectorsReader.getFloatVectorValues()` checks `rawVectorsReader.getFloatVectorValues(field)` and if it returns a `FloatVectorValues` with `size() == 0`, it falls back to dequantization. The `Lucene99FlatVectorsReader.getFloatVectorValues()` calls `getFieldEntry(field, FLOAT32)` which returns null if the field isn't in its map — and if `entry == null` it returns null. Then the caller (`Lucene104ScalarQuantizedVectorsReader`) would NPE when calling `.size()` on a null.

   **Therefore**: the stub `.vemf` must be a valid file with the correct header/footer, but the field entries section should declare zero vectors for each field. The `readFields` loop writes one entry per field; each entry has a `size` int. If we write `size=0` entries for every KNN field, the reader will construct zero-size `FloatVectorValues` and the fallback triggers. The `.vec` stub data file can then be a valid header+footer with zero bytes of actual vector data.

### File structure summary

**`.vemf` stub** (per segment, per vector field suffix):
```
CODEC_MAGIC (4 bytes)
codec_name = "Lucene99FlatVectorsFormatMeta" (VInt length + UTF8 bytes)
version = 0 (4 bytes)
segment_id (16 bytes)
segment_suffix (VInt length + UTF8 bytes)
[for each KNN float/byte field in fieldInfos: field_number (VInt), size=0 (VInt), 
 vectorDataOffset=0 (VLong), vectorDataLength=0 (VLong), dimension (VInt), 
 similarityFunction ordinal (VInt), plus ordToDoc DISI config bytes]
CODEC_FOOTER (checksum etc.)
```

Actually this is getting complex. The safer approach: use the `FilterDirectory` but instead of synthesising the format's binary content ourselves (which is equally fragile to format changes), **write the stub using the real `Lucene99FlatVectorsWriter`**. Create a throwaway in-memory directory, write a zero-vector segment using the real writer, then serve those stub files from the filter. This way we never hardcode binary format details.

### Revised approach: stub generation via real writer

1. At replica startup (during `fieldsReader()` call), detect which raw vector files are absent.
2. For each absent file's corresponding format+segment, call `vectorsFormat.fieldsWriter(state)` against a `ByteBuffersDirectory` (in-memory), add no documents, flush/close — this produces a valid empty `.vemf` and `.vec` in memory.
3. The `FilterDirectory` serves those in-memory bytes for the missing file names.

This is completely upgrade-safe: we use the real writer to produce the stubs, so if the format changes the writer changes with it and the stubs remain valid.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/main/java/com/yelp/nrtsearch/server/codec/RawVectorStubDirectory.java` | **Create** | `FilterDirectory` that synthesises stub raw-vector files using the real format writer |
| `src/main/java/com/yelp/nrtsearch/server/field/VectorFieldDef.java` | **Modify** | Replace hardcoded reader chain with `FilterDirectory`-based approach in `fieldsReader()` |
| `src/main/java/com/yelp/nrtsearch/server/codec/NoOpFlatVectorsReader.java` | **Delete** | No longer needed |
| `src/test/java/com/yelp/nrtsearch/server/codec/RawVectorStubDirectoryTest.java` | **Create** | Unit tests for stub directory |
| `src/test/java/com/yelp/nrtsearch/server/codec/NoOpFlatVectorsReaderTest.java` | **Delete** | Tests for deleted class |
| `src/test/java/com/yelp/nrtsearch/server/field/VectorFieldDefTest.java` | **Modify** | Update `testSkipRawVectorData_*` tests |

---

## Task 1: Create `RawVectorStubDirectory`

**Files:**
- Create: `src/main/java/com/yelp/nrtsearch/server/codec/RawVectorStubDirectory.java`
- Create: `src/test/java/com/yelp/nrtsearch/server/codec/RawVectorStubDirectoryTest.java`

### Context

`RawVectorStubDirectory` is a `FilterDirectory` that wraps the segment's real directory. When a caller tries to open a raw-vector file (`.vec` or `.vemf`) that does not exist in the underlying directory, it synthesises a valid-but-empty stub using the real format's writer, then returns an `IndexInput` over the in-memory bytes.

It is created inside `fieldsReader()` in `VectorFieldDef`'s anonymous `KnnVectorsFormat` wrapper, wrapping `state.directory` before the real `vectorsFormat.fieldsReader(state)` call.

The stub is generated by:
1. Creating a `ByteBuffersDirectory` (Lucene's in-memory directory, in `org.apache.lucene.store`).
2. Constructing a minimal `SegmentWriteState` aimed at that directory.
3. Calling `flatVectorsFormat.fieldsWriter(writeState)` where `flatVectorsFormat` is the `Lucene99FlatVectorsFormat` instance obtained from `FlatVectorScorerUtil.getLucene99FlatVectorsScorer()`.
4. Immediately calling `writer.finish()` and `writer.close()` without adding any fields — this writes a valid empty header+footer.
5. Caching the resulting in-memory bytes keyed by file name so subsequent opens of the same stub are served from cache.

The per-field suffix (e.g. `_0_Lucene99HnswVectorsFormat_0`) embedded in the file name is passed through to the `SegmentWriteState` as the `segmentSuffix`. This ensures the codec header encodes the correct suffix, which `CodecUtil.checkIndexHeader` validates on read.

- [ ] **Step 1: Write the failing test**

```java
// src/test/java/com/yelp/nrtsearch/server/codec/RawVectorStubDirectoryTest.java
package com.yelp.nrtsearch.server.codec;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.junit.Test;

public class RawVectorStubDirectoryTest {

  @Test
  public void testOpenMissingVemfReturnsStub() throws IOException {
    // A directory with no files at all
    Directory empty = new ByteBuffersDirectory();
    SegmentInfo segInfo =
        new SegmentInfo(
            empty,
            Version.LATEST,
            Version.LATEST,
            "test_seg",
            0,
            false,
            false,
            Codec.getDefault(),
            java.util.Collections.emptyMap(),
            org.apache.lucene.util.StringHelper.randomId(),
            java.util.Collections.emptyMap(),
            null);
    SegmentReadState readState =
        new SegmentReadState(empty, segInfo, new FieldInfos(new FieldInfo[0]), IOContext.DEFAULT);

    Lucene99FlatVectorsFormat flatFormat =
        new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
    RawVectorStubDirectory stubDir =
        new RawVectorStubDirectory(empty, readState, flatFormat);

    String vemfName =
        IndexFileNames.segmentFileName(segInfo.name, readState.segmentSuffix, "vemf");

    // File does not exist in the underlying directory
    assertFalse(java.util.Arrays.asList(empty.listAll()).contains(vemfName));

    // But opening it through the stub directory succeeds
    IndexInput in = stubDir.openInput(vemfName, IOContext.DEFAULT);
    assertNotNull(in);
    assertTrue("stub must have at least a codec header+footer", in.length() > 0);
    in.close();
    stubDir.close();
  }

  @Test
  public void testOpenMissingVecReturnsStub() throws IOException {
    Directory empty = new ByteBuffersDirectory();
    SegmentInfo segInfo =
        new SegmentInfo(
            empty,
            Version.LATEST,
            Version.LATEST,
            "test_seg",
            0,
            false,
            false,
            Codec.getDefault(),
            java.util.Collections.emptyMap(),
            org.apache.lucene.util.StringHelper.randomId(),
            java.util.Collections.emptyMap(),
            null);
    SegmentReadState readState =
        new SegmentReadState(empty, segInfo, new FieldInfos(new FieldInfo[0]), IOContext.DEFAULT);
    Lucene99FlatVectorsFormat flatFormat =
        new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
    RawVectorStubDirectory stubDir =
        new RawVectorStubDirectory(empty, readState, flatFormat);

    String vecName =
        IndexFileNames.segmentFileName(segInfo.name, readState.segmentSuffix, "vec");
    IndexInput in = stubDir.openInput(vecName, IOContext.DEFAULT);
    assertNotNull(in);
    assertTrue("stub must have at least a codec header+footer", in.length() > 0);
    in.close();
    stubDir.close();
  }

  @Test
  public void testOpenExistingFilePassesThrough() throws IOException {
    // A real file in the directory should not be stubbed
    ByteBuffersDirectory real = new ByteBuffersDirectory();
    try (IndexOutput out = real.createOutput("some_file.si", IOContext.DEFAULT)) {
      out.writeInt(42);
    }
    SegmentInfo segInfo =
        new SegmentInfo(
            real,
            Version.LATEST,
            Version.LATEST,
            "test_seg",
            0,
            false,
            false,
            Codec.getDefault(),
            java.util.Collections.emptyMap(),
            org.apache.lucene.util.StringHelper.randomId(),
            java.util.Collections.emptyMap(),
            null);
    SegmentReadState readState =
        new SegmentReadState(real, segInfo, new FieldInfos(new FieldInfo[0]), IOContext.DEFAULT);
    Lucene99FlatVectorsFormat flatFormat =
        new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
    RawVectorStubDirectory stubDir =
        new RawVectorStubDirectory(real, readState, flatFormat);

    // Reading the real file returns real data
    IndexInput in = stubDir.openInput("some_file.si", IOContext.DEFAULT);
    assertEquals(Integer.BYTES, in.length());
    in.close();
    stubDir.close();
  }

  @Test
  public void testNonRawVectorMissingFileThrows() throws IOException {
    Directory empty = new ByteBuffersDirectory();
    SegmentInfo segInfo =
        new SegmentInfo(
            empty,
            Version.LATEST,
            Version.LATEST,
            "test_seg",
            0,
            false,
            false,
            Codec.getDefault(),
            java.util.Collections.emptyMap(),
            org.apache.lucene.util.StringHelper.randomId(),
            java.util.Collections.emptyMap(),
            null);
    SegmentReadState readState =
        new SegmentReadState(empty, segInfo, new FieldInfos(new FieldInfo[0]), IOContext.DEFAULT);
    Lucene99FlatVectorsFormat flatFormat =
        new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
    RawVectorStubDirectory stubDir =
        new RawVectorStubDirectory(empty, readState, flatFormat);

    // A missing non-vector file should still throw NoSuchFileException
    try {
      stubDir.openInput("missing_file.si", IOContext.DEFAULT);
      fail("Expected NoSuchFileException");
    } catch (java.nio.file.NoSuchFileException e) {
      // expected
    }
    stubDir.close();
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
./gradlew test --tests "com.yelp.nrtsearch.server.codec.RawVectorStubDirectoryTest" \
  -x :clientlib:test --rerun
```

Expected: `BUILD FAILED` — `RawVectorStubDirectory` does not exist yet.

- [ ] **Step 3: Implement `RawVectorStubDirectory`**

```java
// src/main/java/com/yelp/nrtsearch/server/codec/RawVectorStubDirectory.java
package com.yelp.nrtsearch.server.codec;

import com.yelp.nrtsearch.server.nrt.VectorFileFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

/**
 * A {@link FilterDirectory} used on replicas that have skipped downloading raw vector data files
 * ({@code .vec} / {@code .vemf}). When a raw vector file is absent from the underlying directory,
 * this class synthesises a valid-but-empty stub by running the real {@link FlatVectorsFormat}
 * writer against an in-memory directory with no documents.
 *
 * <p>The stub contains a valid codec header and footer with zero field entries, causing {@code
 * Lucene104ScalarQuantizedVectorsReader.getFloatVectorValues()} to see {@code size()=0} and fall
 * back to reconstructing approximate float vectors from quantized data.
 *
 * <p>Stubs are generated lazily and cached in memory for the lifetime of this directory instance.
 */
public class RawVectorStubDirectory extends FilterDirectory {

  private final SegmentReadState readState;
  private final FlatVectorsFormat flatVectorsFormat;
  private final Map<String, byte[]> stubCache = new HashMap<>();

  public RawVectorStubDirectory(
      Directory delegate, SegmentReadState readState, FlatVectorsFormat flatVectorsFormat) {
    super(delegate);
    this.readState = readState;
    this.flatVectorsFormat = flatVectorsFormat;
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    if (VectorFileFilter.isRawVectorFile(name) && !fileExistsInDelegate(name)) {
      return openStub(name, context);
    }
    return super.openInput(name, context);
  }

  @Override
  public org.apache.lucene.store.ChecksumIndexInput openChecksumInput(String name)
      throws IOException {
    if (VectorFileFilter.isRawVectorFile(name) && !fileExistsInDelegate(name)) {
      // Wrap the stub bytes in a BufferedChecksumIndexInput
      IndexInput stub = openStub(name, IOContext.DEFAULT);
      return new org.apache.lucene.store.BufferedChecksumIndexInput(stub);
    }
    return super.openChecksumInput(name);
  }

  private boolean fileExistsInDelegate(String name) {
    try {
      String[] files = in.listAll();
      for (String f : files) {
        if (f.equals(name)) return true;
      }
      return false;
    } catch (IOException e) {
      return false;
    }
  }

  private synchronized IndexInput openStub(String name, IOContext context) throws IOException {
    byte[] cached = stubCache.get(name);
    if (cached == null) {
      cached = generateStubs();
    }
    byte[] data = stubCache.get(name);
    if (data == null) {
      // stub generation did not produce this file — fall back to delegate (will throw if absent)
      return super.openInput(name, context);
    }
    return new org.apache.lucene.store.ByteBuffersIndexInput(
        new org.apache.lucene.store.ByteBuffersDataInput(
            java.util.List.of(java.nio.ByteBuffer.wrap(data))),
        name);
  }

  /**
   * Generate empty stub files for this segment using the real format writer, cache their bytes,
   * and return the bytes for the first file requested.
   */
  private byte[] generateStubs() throws IOException {
    ByteBuffersDirectory stubDir = new ByteBuffersDirectory();

    SegmentInfo segInfo = readState.segmentInfo;
    SegmentWriteState writeState =
        new SegmentWriteState(
            null, // InfoStream — null is fine for write
            stubDir,
            segInfo,
            readState.fieldInfos,
            null, // BufferedUpdates — not needed
            readState.context,
            readState.segmentSuffix);

    FlatVectorsWriter writer = flatVectorsFormat.fieldsWriter(writeState);
    writer.finish();
    writer.close();

    // Cache all generated files
    for (String file : stubDir.listAll()) {
      try (IndexInput in = stubDir.openInput(file, IOContext.DEFAULT)) {
        byte[] bytes = new byte[(int) in.length()];
        in.readBytes(bytes, 0, bytes.length);
        stubCache.put(file, bytes);
      }
    }
    stubDir.close();
    return null; // caller re-reads from stubCache
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
./gradlew spotlessApply && ./gradlew test \
  --tests "com.yelp.nrtsearch.server.codec.RawVectorStubDirectoryTest" \
  -x :clientlib:test --rerun
```

Expected: `BUILD SUCCESSFUL`, all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/yelp/nrtsearch/server/codec/RawVectorStubDirectory.java \
        src/test/java/com/yelp/nrtsearch/server/codec/RawVectorStubDirectoryTest.java
git commit -m "feat: add RawVectorStubDirectory for upgrade-safe raw vector skipping"
```

---

## Task 2: Wire `RawVectorStubDirectory` into `VectorFieldDef` and remove `NoOpFlatVectorsReader`

**Files:**
- Modify: `src/main/java/com/yelp/nrtsearch/server/field/VectorFieldDef.java` (lines ~210–234)
- Delete: `src/main/java/com/yelp/nrtsearch/server/codec/NoOpFlatVectorsReader.java`
- Delete: `src/test/java/com/yelp/nrtsearch/server/codec/NoOpFlatVectorsReaderTest.java`
- Modify: `src/test/java/com/yelp/nrtsearch/server/field/VectorFieldDefTest.java`

### Context

The `fieldsReader()` override in `VectorFieldDef.createVectorsFormat()` currently constructs the reader chain by class name. Replace it: when `skipRawVectors=true`, wrap `state.directory` with a `RawVectorStubDirectory` and call the real `vectorsFormat.fieldsReader(state)` using the wrapped state. Remove the `NoOpFlatVectorsReader` class and its test entirely.

The `Lucene99FlatVectorsFormat` instance needed by `RawVectorStubDirectory` is obtained via `FlatVectorScorerUtil.getLucene99FlatVectorsScorer()`. This must be captured as a variable at format-creation time (inside `createVectorsFormat()`) so it's available inside the anonymous class's `fieldsReader()` lambda.

- [ ] **Step 1: Update the test expectations**

In `VectorFieldDefTest.java`, the test `testSkipRawVectorData_quantizedFieldFormatUnchanged` just verifies that `format.toString()` is the same with and without the skip flag — it still holds. No change needed there.

Remove `testSkipRawVectorData_nonQuantizedFieldUnaffected` if it still exists — the logic is unchanged (non-quantized fields are never wrapped). Verify this test still passes as-is; if it does, no edit is required.

Run existing tests to confirm they still pass (they will fail once we delete `NoOpFlatVectorsReader`):

```bash
./gradlew test --tests "com.yelp.nrtsearch.server.field.VectorFieldDefTest.testSkipRawVectorData*" \
  -x :clientlib:test --rerun
```

Expected at this point: `BUILD SUCCESSFUL` (tests still reference the old approach, not yet modified).

- [ ] **Step 2: Update `VectorFieldDef.createVectorsFormat()` to use `RawVectorStubDirectory`**

Replace the `fieldsReader()` body in `VectorFieldDef.java`. The diff from lines 210–233 should change from:

```java
// BEFORE
NrtsearchConfig config = fieldDefCreatorContext.config();
boolean skipRawVectors =
    config != null
        && config.getSkipRawVectorData()
        && vectorSearchType == VectorSearchType.HNSW_SCALAR_QUANTIZED;

return new KnnVectorsFormat(vectorsFormat.getName()) {
  ...
  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    if (skipRawVectors) {
      return new Lucene99HnswVectorsReader(
          state,
          new Lucene104ScalarQuantizedVectorsReader(
              state,
              new NoOpFlatVectorsReader(),
              new Lucene104ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE)));
    }
    return vectorsFormat.fieldsReader(state);
  }
```

To:

```java
// AFTER
NrtsearchConfig config = fieldDefCreatorContext.config();
boolean skipRawVectors =
    config != null
        && config.getSkipRawVectorData()
        && vectorSearchType == VectorSearchType.HNSW_SCALAR_QUANTIZED;
Lucene99FlatVectorsFormat flatVectorsFormat =
    skipRawVectors
        ? new Lucene99FlatVectorsFormat(
            FlatVectorScorerUtil.getLucene99FlatVectorsScorer())
        : null;

return new KnnVectorsFormat(vectorsFormat.getName()) {
  ...
  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    if (skipRawVectors) {
      SegmentReadState wrappedState =
          new SegmentReadState(
              new RawVectorStubDirectory(state.directory, state, flatVectorsFormat),
              state.segmentInfo,
              state.fieldInfos,
              state.context,
              state.segmentSuffix);
      return vectorsFormat.fieldsReader(wrappedState);
    }
    return vectorsFormat.fieldsReader(state);
  }
```

Also remove the now-unused imports:
- `import com.yelp.nrtsearch.server.codec.NoOpFlatVectorsReader;`
- `import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;`
- `import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;`
- `import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsReader;`
- `import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;`

Add new imports:
- `import com.yelp.nrtsearch.server.codec.RawVectorStubDirectory;`
- `import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;`
- `import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;`

- [ ] **Step 3: Delete `NoOpFlatVectorsReader` and its test**

```bash
rm src/main/java/com/yelp/nrtsearch/server/codec/NoOpFlatVectorsReader.java
rm src/test/java/com/yelp/nrtsearch/server/codec/NoOpFlatVectorsReaderTest.java
```

- [ ] **Step 4: Run all affected tests**

```bash
./gradlew spotlessApply && ./gradlew test \
  --tests "com.yelp.nrtsearch.server.field.VectorFieldDefTest.testSkipRawVectorData*" \
  --tests "com.yelp.nrtsearch.server.codec.RawVectorStubDirectoryTest" \
  --tests "com.yelp.nrtsearch.server.nrt.VectorFileFilterTest" \
  -x :clientlib:test --rerun
```

Expected: `BUILD SUCCESSFUL`, all tests pass.

- [ ] **Step 5: Run full regression over affected modules**

```bash
./gradlew test \
  --tests "com.yelp.nrtsearch.server.field.VectorFieldDefTest" \
  --tests "com.yelp.nrtsearch.server.codec.*" \
  --tests "com.yelp.nrtsearch.server.nrt.*" \
  --tests "com.yelp.nrtsearch.server.config.NrtsearchConfigTest" \
  --tests "com.yelp.nrtsearch.server.index.ImmutableIndexStateTest" \
  -x :clientlib:test --rerun
```

Expected: `BUILD SUCCESSFUL`, no regressions.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/yelp/nrtsearch/server/field/VectorFieldDef.java
git rm src/main/java/com/yelp/nrtsearch/server/codec/NoOpFlatVectorsReader.java
git rm src/test/java/com/yelp/nrtsearch/server/codec/NoOpFlatVectorsReaderTest.java
git commit -m "refactor: replace hardcoded codec reader chain with FilterDirectory stub approach"
```

---

## Self-review

### Spec coverage

| Requirement | Covered by |
|-------------|------------|
| No hardcoded reader class names in nrtsearch bypass path | Task 2: `vectorsFormat.fieldsReader(wrappedState)` used |
| Real format's `fieldsReader` called (version-correct reader) | Task 2: the real `vectorsFormat` is delegated to |
| Stubs produced by real writer (upgrade-safe content) | Task 1: `flatVectorsFormat.fieldsWriter(writeState)` writes stubs |
| Non-raw-vector missing files still throw | Task 1: `testNonRawVectorMissingFileThrows` |
| Existing real files pass through unchanged | Task 1: `testOpenExistingFilePassesThrough` |
| Replication-layer filtering unchanged | Not touched — `VectorFileFilter`, `NrtDataManager`, `RemoteCopyJobManager`, `GrpcCopyJobManager` all unchanged |
| `NoOpFlatVectorsReader` removed | Task 2 step 3 |
| All existing tests pass | Task 2 steps 4–5 |

### Placeholder scan

None found — all steps contain complete code.

### Type consistency

- `RawVectorStubDirectory` constructor: `(Directory, SegmentReadState, FlatVectorsFormat)` — used identically in Task 1 (tests) and Task 2 (VectorFieldDef).
- `flatVectorsFormat` captured as `Lucene99FlatVectorsFormat` in Task 2 — passed as `FlatVectorsFormat` in constructor (subtype, valid).
- `SegmentReadState` constructor used in Task 2: `(Directory, SegmentInfo, FieldInfos, IOContext, String)` — this is the standard constructor form; verify the exact signature matches Lucene 10.4's `SegmentReadState` before implementation (there are multiple overloads).

### Known risks for the implementer

1. **`SegmentWriteState` constructor with null `InfoStream`** — Lucene's `FlatVectorsWriter` may call `infoStream.message(...)`. Pass `InfoStream.NO_OUTPUT` rather than `null` to be safe.

2. **`SegmentWriteState` requires `fieldInfos` with no KNN fields** — if the write state is constructed with `fieldInfos` that contain KNN vector fields, the writer will try to write entries for them with no data, which may fail. Pass `new FieldInfos(new FieldInfo[0])` (empty field infos) to the write state, not the read state's field infos, so the writer produces a clean empty file.

3. **`BufferedChecksumIndexInput`** — `openChecksumInput` override wraps the stub in a `BufferedChecksumIndexInput`. Verify this class is in `org.apache.lucene.store` in Lucene 10.4 (it has been stable across versions).

4. **Thread safety** — `stubCache` is a `HashMap` accessed under `synchronized (this)` in `openStub`. The `generateStubs()` call happens inside the synchronized block, which means the first open of a missing file blocks briefly while stubs are generated. This is acceptable since stub generation only occurs at segment open time.
