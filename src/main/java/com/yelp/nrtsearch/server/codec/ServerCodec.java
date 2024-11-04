/*
 * Copyright 2020 Yelp Inc.
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

import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.field.VectorFieldDef;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;

/** Implements per-index {@link Codec}. */
public class ServerCodec extends Lucene912Codec {
  private final IndexStateManager stateManager;

  // nocommit expose compression control

  /** Sole constructor. */
  public ServerCodec(IndexStateManager stateManager) {
    this.stateManager = stateManager;
  }

  @Override
  public PostingsFormat getPostingsFormatForField(String field) {
    IndexState state = stateManager.getCurrent();
    try {
      FieldDef fd = state.getFieldOrThrow(field);
      if (fd instanceof IndexableFieldDef<?> indexableFieldDef) {
        PostingsFormat postingsFormat = indexableFieldDef.getPostingsFormat();
        if (postingsFormat != null) {
          return postingsFormat;
        }
      } else {
        throw new IllegalArgumentException("Field \"" + field + "\" is not indexable");
      }
    } catch (IllegalArgumentException iae) {
      // The indexed facets field will have drill-downs,
      // which will pull the postings format:
      if (!state.getInternalFacetFieldNames().contains(field)) {
        throw iae;
      }
    }
    return super.getPostingsFormatForField(field);
  }

  @Override
  public DocValuesFormat getDocValuesFormatForField(String field) {
    IndexState state = stateManager.getCurrent();
    try {
      FieldDef fd = state.getFieldOrThrow(field);
      if (fd instanceof IndexableFieldDef<?> indexableFieldDef) {
        DocValuesFormat docValuesFormat = indexableFieldDef.getDocValuesFormat();
        if (docValuesFormat != null) {
          return docValuesFormat;
        }
      } else {
        throw new IllegalArgumentException("Field \"" + field + "\" is not indexable");
      }
    } catch (IllegalArgumentException iae) {
      if (!state.getInternalFacetFieldNames().contains(field)) {
        throw iae;
      }
    }
    return super.getDocValuesFormatForField(field);
  }

  @Override
  public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
    IndexState state = stateManager.getCurrent();
    try {
      FieldDef fd = state.getFieldOrThrow(field);
      if (fd instanceof VectorFieldDef<?> vectorFieldDef) {
        KnnVectorsFormat vectorsFormat = vectorFieldDef.getVectorsFormat();
        if (vectorsFormat != null) {
          return vectorsFormat;
        }
      }
    } catch (IllegalArgumentException ignored) {
    }
    return super.getKnnVectorsFormatForField(field);
  }
}
