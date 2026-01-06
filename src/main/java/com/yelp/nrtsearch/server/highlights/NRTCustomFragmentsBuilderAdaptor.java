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
package com.yelp.nrtsearch.server.highlights;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.BaseFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo.SubInfo;
import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo.Toffs;

/**
 * Adapter for {@link org.apache.lucene.search.vectorhighlight.FragmentsBuilder} that highlights
 * only the top matched phrases based on the boost value in the query. This adapter does not alter
 * the order or score of the generated fragments. All phrases contribute to scoring if the
 * innerBaseFragmentsBuilder is a {@link
 * org.apache.lucene.search.vectorhighlight.ScoreOrderFragmentsBuilder}.
 */
public class NRTCustomFragmentsBuilderAdaptor extends BaseFragmentsBuilder {

  private final BaseFragmentsBuilder innerBaseFragmentsBuilder;
  private final boolean topBoostOnly;
  private final int maxNumberOfHighlightedPhrasePerFragment;

  /** a constructor. */
  public NRTCustomFragmentsBuilderAdaptor(
      BaseFragmentsBuilder baseFragmentsBuilder,
      BoundaryScanner boundaryScanner,
      boolean topBoostOnly,
      int maxNumberOfHighlightedPhrasePerFragment) {
    super(boundaryScanner);
    this.innerBaseFragmentsBuilder = baseFragmentsBuilder;
    this.topBoostOnly = topBoostOnly;
    this.maxNumberOfHighlightedPhrasePerFragment = maxNumberOfHighlightedPhrasePerFragment;
  }

  /** Delegates the inner FragmentsBuilder to determine the fragment order. */
  @Override
  public List<WeightedFragInfo> getWeightedFragInfoList(List<WeightedFragInfo> src) {
    return innerBaseFragmentsBuilder.getWeightedFragInfoList(src);
  }

  /**
   * Creates a fragment containing only the top boost phrase if the `topBoostOnly` flag is set.
   * Otherwise, it delegates to the base implementation.
   */
  @Override
  protected String makeFragment(
      StringBuilder buffer,
      int[] index,
      Field[] values,
      WeightedFragInfo fragInfo,
      String[] preTags,
      String[] postTags,
      Encoder encoder) {
    if ((!topBoostOnly) && (maxNumberOfHighlightedPhrasePerFragment == 0)) {
      return super.makeFragment(buffer, index, values, fragInfo, preTags, postTags, encoder);
    }
    StringBuilder fragment = new StringBuilder();
    final int s = fragInfo.getStartOffset();
    int[] modifiedStartOffset = {s};
    String src =
        getFragmentSourceMSO(
            buffer, index, values, s, fragInfo.getEndOffset(), modifiedStartOffset);
    int srcIndex = 0;

    Stream<SubInfo> subInfoStream = fragInfo.getSubInfos().stream();

    if (topBoostOnly) {
      float topBoostValue =
          fragInfo.getSubInfos().stream().map(SubInfo::boost).max(Float::compare).orElse(0f);
      subInfoStream = subInfoStream.filter(subInfo -> subInfo.boost() >= topBoostValue);
    }
    if (maxNumberOfHighlightedPhrasePerFragment > 0) {
      subInfoStream =
          subInfoStream
              .sorted(
                  (a, b) ->
                      a.boost() == b.boost()
                          ? a.termsOffsets().getFirst().getStartOffset()
                              - b.termsOffsets().getFirst().getStartOffset()
                          : Float.compare(b.boost(), a.boost()))
              .limit(maxNumberOfHighlightedPhrasePerFragment)
              // revert back to the original order as this is required when creating the fragments
              .sorted(Comparator.comparingInt(a -> a.termsOffsets().getFirst().getStartOffset()));
    }

    for (SubInfo subInfo : subInfoStream.toList()) {
      for (Toffs to : subInfo.termsOffsets()) {
        fragment
            .append(
                encoder.encodeText(
                    src.substring(srcIndex, to.getStartOffset() - modifiedStartOffset[0])))
            .append(getPreTag(preTags, subInfo.seqnum()))
            .append(
                encoder.encodeText(
                    src.substring(
                        to.getStartOffset() - modifiedStartOffset[0],
                        to.getEndOffset() - modifiedStartOffset[0])))
            .append(getPostTag(postTags, subInfo.seqnum()));
        srcIndex = to.getEndOffset() - modifiedStartOffset[0];
      }
    }
    fragment.append(encoder.encodeText(src.substring(srcIndex)));
    return fragment.toString();
  }
}
