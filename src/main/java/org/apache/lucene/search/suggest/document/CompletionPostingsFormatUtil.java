/*
 * Copyright 2022 Yelp Inc.
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
package org.apache.lucene.search.suggest.document;

import org.apache.lucene.search.suggest.document.CompletionPostingsFormat.FSTLoadMode;

/** Utility class for setting properties of the suggest completion codec. */
public class CompletionPostingsFormatUtil {
  private static FSTLoadMode completionCodecLoadMode = FSTLoadMode.ON_HEAP;

  private CompletionPostingsFormatUtil() {}

  /**
   * Set the FST load mode used by the modified {@link Completion84PostingsFormat}. Must be set
   * before any index data is loaded.
   *
   * @param loadMode new FST load mode
   */
  public static void setCompletionCodecLoadMode(FSTLoadMode loadMode) {
    completionCodecLoadMode = loadMode;
  }

  /** Get the current FST load mode for completion codecs. */
  public static FSTLoadMode getCompletionCodecLoadMode() {
    return completionCodecLoadMode;
  }
}
