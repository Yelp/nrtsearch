/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.highlights;

import java.util.List;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.BaseFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo.SubInfo;
import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo.Toffs;

public class TopBoostOnlyFragmentsBuilderAdaptor extends BaseFragmentsBuilder {
  private final BaseFragmentsBuilder innerBaseFragmentsBuilder;
  private final boolean topBoostOnly;

  /** a constructor. */
  public TopBoostOnlyFragmentsBuilderAdaptor(
      BaseFragmentsBuilder baseFragmentsBuilder,
      BoundaryScanner boundaryScanner,
      boolean topBoostOnly) {
    super(boundaryScanner);
    this.innerBaseFragmentsBuilder = baseFragmentsBuilder;
    this.topBoostOnly = topBoostOnly;
  }

  @Override
  public List<WeightedFragInfo> getWeightedFragInfoList(List<WeightedFragInfo> src) {
    return innerBaseFragmentsBuilder.getWeightedFragInfoList(src);
  }

  @Override
  protected String makeFragment(
      StringBuilder buffer,
      int[] index,
      Field[] values,
      WeightedFragInfo fragInfo,
      String[] preTags,
      String[] postTags,
      Encoder encoder) {
    if (!topBoostOnly) {
      return super.makeFragment(buffer, index, values, fragInfo, preTags, postTags, encoder);
    }
    StringBuilder fragment = new StringBuilder();
    final int s = fragInfo.getStartOffset();
    int[] modifiedStartOffset = {s};
    String src =
        getFragmentSourceMSO(
            buffer, index, values, s, fragInfo.getEndOffset(), modifiedStartOffset);
    int srcIndex = 0;
    double topBoostValue =
        fragInfo.getSubInfos().stream().map(SubInfo::getBoost).max(Float::compare).orElse(0f);
    for (SubInfo subInfo : fragInfo.getSubInfos()) {
      if (subInfo.getBoost() < topBoostValue) {
        continue;
      }
      for (Toffs to : subInfo.getTermsOffsets()) {
        fragment
            .append(
                encoder.encodeText(
                    src.substring(srcIndex, to.getStartOffset() - modifiedStartOffset[0])))
            .append(getPreTag(preTags, subInfo.getSeqnum()))
            .append(
                encoder.encodeText(
                    src.substring(
                        to.getStartOffset() - modifiedStartOffset[0],
                        to.getEndOffset() - modifiedStartOffset[0])))
            .append(getPostTag(postTags, subInfo.getSeqnum()));
        srcIndex = to.getEndOffset() - modifiedStartOffset[0];
      }
    }
    fragment.append(encoder.encodeText(src.substring(srcIndex)));
    return fragment.toString();
  }
}
