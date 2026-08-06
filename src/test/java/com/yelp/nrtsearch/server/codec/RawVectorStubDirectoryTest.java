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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class RawVectorStubDirectoryTest {

  @Test
  public void testOpenMissingVemfReturnsStub() throws IOException {
    ByteBuffersDirectory empty = new ByteBuffersDirectory();
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
            Collections.emptyMap(),
            StringHelper.randomId(),
            Collections.emptyMap(),
            null);
    SegmentReadState readState =
        new SegmentReadState(empty, segInfo, new FieldInfos(new FieldInfo[0]), IOContext.DEFAULT);
    Lucene99FlatVectorsFormat flatFormat =
        new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
    RawVectorStubDirectory stubDir = new RawVectorStubDirectory(empty, readState, flatFormat);
    String vemfName = IndexFileNames.segmentFileName(segInfo.name, readState.segmentSuffix, "vemf");
    assertFalse(Arrays.asList(empty.listAll()).contains(vemfName));
    IndexInput in = stubDir.openInput(vemfName, IOContext.DEFAULT);
    assertNotNull(in);
    assertTrue("stub must have at least a codec header+footer", in.length() > 0);
    in.close();
    stubDir.close();
  }

  @Test
  public void testOpenMissingVecReturnsStub() throws IOException {
    ByteBuffersDirectory empty = new ByteBuffersDirectory();
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
            Collections.emptyMap(),
            StringHelper.randomId(),
            Collections.emptyMap(),
            null);
    SegmentReadState readState =
        new SegmentReadState(empty, segInfo, new FieldInfos(new FieldInfo[0]), IOContext.DEFAULT);
    Lucene99FlatVectorsFormat flatFormat =
        new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
    RawVectorStubDirectory stubDir = new RawVectorStubDirectory(empty, readState, flatFormat);
    String vecName = IndexFileNames.segmentFileName(segInfo.name, readState.segmentSuffix, "vec");
    IndexInput in = stubDir.openInput(vecName, IOContext.DEFAULT);
    assertNotNull(in);
    assertTrue("stub must have at least a codec header+footer", in.length() > 0);
    in.close();
    stubDir.close();
  }

  @Test
  public void testOpenExistingFilePassesThrough() throws IOException {
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
            Collections.emptyMap(),
            StringHelper.randomId(),
            Collections.emptyMap(),
            null);
    SegmentReadState readState =
        new SegmentReadState(real, segInfo, new FieldInfos(new FieldInfo[0]), IOContext.DEFAULT);
    Lucene99FlatVectorsFormat flatFormat =
        new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
    RawVectorStubDirectory stubDir = new RawVectorStubDirectory(real, readState, flatFormat);
    IndexInput in = stubDir.openInput("some_file.si", IOContext.DEFAULT);
    assertEquals(Integer.BYTES, in.length());
    in.close();
    stubDir.close();
  }

  @Test
  public void testNonRawVectorMissingFileThrows() throws IOException {
    ByteBuffersDirectory empty = new ByteBuffersDirectory();
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
            Collections.emptyMap(),
            StringHelper.randomId(),
            Collections.emptyMap(),
            null);
    SegmentReadState readState =
        new SegmentReadState(empty, segInfo, new FieldInfos(new FieldInfo[0]), IOContext.DEFAULT);
    Lucene99FlatVectorsFormat flatFormat =
        new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
    RawVectorStubDirectory stubDir = new RawVectorStubDirectory(empty, readState, flatFormat);
    try {
      stubDir.openInput("missing_file.si", IOContext.DEFAULT);
      fail("Expected NoSuchFileException");
    } catch (NoSuchFileException e) {
      // expected
    }
    stubDir.close();
  }
}
