/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.codec;

import com.yelp.nrtsearch.server.nrt.VectorFileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.InfoStream;

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
      org.apache.lucene.store.Directory delegate,
      SegmentReadState readState,
      FlatVectorsFormat flatVectorsFormat) {
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
  public ChecksumIndexInput openChecksumInput(String name) throws IOException {
    if (VectorFileFilter.isRawVectorFile(name) && !fileExistsInDelegate(name)) {
      return new BufferedChecksumIndexInput(openStub(name, IOContext.DEFAULT));
    }
    return super.openChecksumInput(name);
  }

  private boolean fileExistsInDelegate(String name) {
    try {
      for (String f : in.listAll()) {
        if (f.equals(name)) return true;
      }
      return false;
    } catch (IOException e) {
      return false;
    }
  }

  private synchronized IndexInput openStub(String name, IOContext context) throws IOException {
    if (!stubCache.containsKey(name)) {
      generateStubs();
    }
    byte[] data = stubCache.get(name);
    if (data == null) {
      return super.openInput(name, context);
    }
    return new ByteBuffersIndexInput(
        new ByteBuffersDataInput(List.of(ByteBuffer.wrap(data))), name);
  }

  private void generateStubs() throws IOException {
    ByteBuffersDirectory stubDir = new ByteBuffersDirectory();
    SegmentInfo segInfo = readState.segmentInfo;
    SegmentWriteState writeState =
        new SegmentWriteState(
            InfoStream.NO_OUTPUT,
            stubDir,
            segInfo,
            new FieldInfos(new FieldInfo[0]),
            null,
            readState.context,
            readState.segmentSuffix);
    FlatVectorsWriter writer = flatVectorsFormat.fieldsWriter(writeState);
    writer.finish();
    writer.close();
    for (String file : stubDir.listAll()) {
      try (IndexInput fileIn = stubDir.openInput(file, IOContext.DEFAULT)) {
        byte[] bytes = new byte[(int) fileIn.length()];
        fileIn.readBytes(bytes, 0, bytes.length);
        stubCache.put(file, bytes);
      }
    }
    stubDir.close();
  }
}
