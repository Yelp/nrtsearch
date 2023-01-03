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

import org.apache.lucene.codecs.PostingsFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copy of the lucene Completion84PostingsFormat, but allows the FST load mode to be configured.
 * Since this codec is loaded by class name, it must have the same name as the original and be
 * present earlier in the class path.
 */
public class Completion84PostingsFormat extends CompletionPostingsFormat {
  private static final Logger logger = LoggerFactory.getLogger(Completion84PostingsFormat.class);
  /**
   * Creates a {@link Completion84PostingsFormat} that will load the completion FST based on the
   * value present in {@link CompletionPostingsFormatUtil}.
   */
  public Completion84PostingsFormat() {
    this(CompletionPostingsFormatUtil.getCompletionCodecLoadMode());
  }

  /**
   * Creates a {@link Completion84PostingsFormat} that will use the provided <code>fstLoadMode
   * </code> to determine if the completion FST should be loaded on or off heap.
   */
  public Completion84PostingsFormat(FSTLoadMode fstLoadMode) {
    super("Completion84", fstLoadMode);
    logger.info("Created Completion84PostingsFormat with fstLoadMode: " + fstLoadMode);
  }

  @Override
  protected PostingsFormat delegatePostingsFormat() {
    return PostingsFormat.forName("Lucene84");
  }
}
