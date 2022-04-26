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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene84.Lucene84Codec;

/** Implements per-index {@link Codec}. */
public class ServerCodec extends Lucene84Codec {

  public static final String DEFAULT_POSTINGS_FORMAT = "Lucene84";
  public static final String DEFAULT_DOC_VALUES_FORMAT = "Lucene80";

  private final IndexStateManager stateManager;
  // nocommit expose compression control

  /** Sole constructor. */
  public ServerCodec(IndexStateManager stateManager) {
    this.stateManager = stateManager;
  }

  @Override
  public PostingsFormat getPostingsFormatForField(String field) {
    IndexState state = stateManager.getCurrent();
    String pf;
    try {
      FieldDef fd = state.getField(field);
      if (fd instanceof IndexableFieldDef) {
        pf = ((IndexableFieldDef) fd).getPostingsFormat();
      } else {
        throw new IllegalArgumentException("Field " + field + " is not indexable");
      }
    } catch (IllegalArgumentException iae) {
      // The indexed facets field will have drill-downs,
      // which will pull the postings format:
      if (state.getInternalFacetFieldNames().contains(field)) {
        return super.getPostingsFormatForField(field);
      } else {
        throw iae;
      }
    }
    return PostingsFormat.forName(pf);
  }

  @Override
  public DocValuesFormat getDocValuesFormatForField(String field) {
    IndexState state = stateManager.getCurrent();
    String dvf;
    try {
      FieldDef fd = state.getField(field);
      if (fd instanceof IndexableFieldDef) {
        dvf = ((IndexableFieldDef) fd).getDocValuesFormat();
      } else {
        throw new IllegalArgumentException("Field " + field + " is not indexable");
      }
    } catch (IllegalArgumentException iae) {
      if (state.getInternalFacetFieldNames().contains(field)) {
        return super.getDocValuesFormatForField(field);
      } else {
        throw iae;
      }
    }
    return DocValuesFormat.forName(dvf);
  }
}
