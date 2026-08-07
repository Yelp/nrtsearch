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
package com.yelp.nrtsearch.server.nrt;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.index.IndexFileNames;

/**
 * Utility for filtering raw (full-precision) vector data files from an index file set. Used when
 * replicas are configured to skip downloading raw vector data and rely solely on quantized vectors.
 *
 * <p>Raw vector files (.vec, .vemf) are written by Lucene99FlatVectorsFormat and are only needed
 * for re-quantization during segment merges (which replicas never perform) or for exact float
 * vector access. Quantized replicas can reconstruct approximate float vectors from .veq/.vemq data.
 *
 * <p>Prerequisite: all vector fields on the index must use hnsw_scalar_quantized. Plain HNSW fields
 * also produce .vec/.vemf files but have no quantized fallback — skipping their raw files will
 * break vector access for those fields.
 */
public class VectorFileFilter {

  private static final Set<String> RAW_VECTOR_EXTENSIONS = Set.of("vec", "vemf");

  private VectorFileFilter() {}

  /**
   * Returns true if the given file name has a raw vector data extension (.vec or .vemf).
   *
   * @param fileName segment file name (may include path prefix)
   * @return true if this is a raw vector file that can be skipped on quantized-only replicas
   */
  public static boolean isRawVectorFile(String fileName) {
    String ext = IndexFileNames.getExtension(fileName);
    return ext != null && RAW_VECTOR_EXTENSIONS.contains(ext);
  }

  /**
   * Returns a new map with all raw vector files (.vec, .vemf) removed.
   *
   * @param files map of file name to metadata
   * @return filtered map excluding raw vector files
   */
  public static <T> Map<String, T> filterRawVectorFiles(Map<String, T> files) {
    return files.entrySet().stream()
        .filter(e -> !isRawVectorFile(e.getKey()))
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
