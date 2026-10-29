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
package com.yelp.nrtsearch.server.nrt.pagecache;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/** Writes a small single segment index with a quantized and a float vector field. */
final class PageCacheTestIndex {
  static final String QUANTIZED_FIELD = "quantized_vector";
  static final String FLOAT_FIELD = "float_vector";
  static final int DIMENSIONS = 64;
  static final int NUM_DOCS = 2000;
  // Sub-file names of the quantized field's full precision vectors, and quantized vectors
  static final String QUANTIZED_VEC = "_Lucene104HnswBinaryQuantizedVectorsFormat_0.vec";
  static final String QUANTIZED_VEQ = "_Lucene104HnswBinaryQuantizedVectorsFormat_0.veq";

  private PageCacheTestIndex() {}

  private static class TestCodec extends Lucene104Codec {
    private final KnnVectorsFormat quantizedFormat =
        new Lucene104HnswScalarQuantizedVectorsFormat();
    private final KnnVectorsFormat floatFormat = new Lucene99HnswVectorsFormat();

    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
      return QUANTIZED_FIELD.equals(field) ? quantizedFormat : floatFormat;
    }
  }

  static void write(Path indexPath, boolean useCompoundFile) throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(new TestCodec());
    iwc.setUseCompoundFile(useCompoundFile);
    Random random = new Random(42);
    try (Directory directory = FSDirectory.open(indexPath);
        IndexWriter writer = new IndexWriter(directory, iwc)) {
      for (int i = 0; i < NUM_DOCS; ++i) {
        Document document = new Document();
        document.add(new StringField("id", String.valueOf(i), StringField.Store.YES));
        document.add(
            new KnnFloatVectorField(
                QUANTIZED_FIELD, randomVector(random), VectorSimilarityFunction.DOT_PRODUCT));
        document.add(
            new KnnFloatVectorField(
                FLOAT_FIELD, randomVector(random), VectorSimilarityFunction.DOT_PRODUCT));
        writer.addDocument(document);
      }
      writer.commit();
    }
  }

  private static float[] randomVector(Random random) {
    float[] vector = new float[DIMENSIONS];
    float norm = 0;
    for (int i = 0; i < DIMENSIONS; ++i) {
      vector[i] = random.nextFloat() - 0.5f;
      norm += vector[i] * vector[i];
    }
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < DIMENSIONS; ++i) {
      vector[i] /= norm;
    }
    return vector;
  }
}
