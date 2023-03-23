/*
 * Copyright 2023 Yelp Inc.
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
package org.apache.lucene.search.vectorhighlight;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo.SubInfo;
import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo.Toffs;

public abstract class MultiValueBaseFragmentsBuilder extends BaseFragmentsBuilder {
  /**
   * a constructor.
   */
  public MultiValueBaseFragmentsBuilder() {
    super();
  }

  /**
   * a constructor.
   *
   * @param preTags array of pre-tags for markup terms.
   * @param postTags array of post-tags for markup terms.
   */
  public MultiValueBaseFragmentsBuilder( String[] preTags, String[] postTags ) {
    super( preTags, postTags );
  }

  public MultiValueBaseFragmentsBuilder( BoundaryScanner bs ) {
    super( bs );
  }

  public MultiValueBaseFragmentsBuilder( String[] preTags, String[] postTags, BoundaryScanner bs ) {
    super( preTags, postTags, bs );
  }
  protected List<WeightedFragInfo> discreteMultiValueHighlighting(List<WeightedFragInfo> fragInfos, Field[] fields) {
    Map<String, List<WeightedFragInfo>> fieldNameToFragInfos = new HashMap<>();
    for (Field field : fields) {
      fieldNameToFragInfos.put(field.name(), new ArrayList<WeightedFragInfo>());
    }

    fragInfos: for (WeightedFragInfo fragInfo : fragInfos) {
      int fieldStart;
      int fieldEnd = 0;
      for (Field field : fields) {
        if (field.stringValue().isEmpty()) {
          fieldEnd++;
          continue;
        }
        fieldStart = fieldEnd;
        fieldEnd += field.stringValue().length() + 1; // + 1 for going to next field with same name.

        if (fragInfo.getStartOffset() >= fieldStart && fragInfo.getEndOffset() >= fieldStart &&
            fragInfo.getStartOffset() <= fieldEnd && fragInfo.getEndOffset() <= fieldEnd) {
          fieldNameToFragInfos.get(field.name()).add(fragInfo);
          continue fragInfos;
        }

        if (fragInfo.getSubInfos().isEmpty()) {
          continue fragInfos;
        }

        Toffs firstToffs = fragInfo.getSubInfos().get(0).getTermsOffsets().get(0);
        if (fragInfo.getStartOffset() >= fieldEnd || firstToffs.getStartOffset() >= fieldEnd) {
          continue;
        }

        int fragStart = fieldStart;
        if (fragInfo.getStartOffset() > fieldStart && fragInfo.getStartOffset() < fieldEnd) {
          fragStart = fragInfo.getStartOffset();
        }

        int fragEnd = fieldEnd;
        if (fragInfo.getEndOffset() > fieldStart && fragInfo.getEndOffset() < fieldEnd) {
          fragEnd = fragInfo.getEndOffset();
        }


        List<SubInfo> subInfos = new ArrayList<>();
        Iterator<SubInfo> subInfoIterator = fragInfo.getSubInfos().iterator();
        float boost = 0.0f;  //  The boost of the new info will be the sum of the boosts of its SubInfos
        while (subInfoIterator.hasNext()) {
          SubInfo subInfo = subInfoIterator.next();
          List<Toffs> toffsList = new ArrayList<>();
          Iterator<Toffs> toffsIterator = subInfo.getTermsOffsets().iterator();
          while (toffsIterator.hasNext()) {
            Toffs toffs = toffsIterator.next();
            if (toffs.getStartOffset() >= fieldEnd) {
              // We've gone past this value so its not worth iterating any more.
              break;
            }
            boolean startsAfterField = toffs.getStartOffset() >= fieldStart;
            boolean endsBeforeField = toffs.getEndOffset() < fieldEnd;
            if (startsAfterField && endsBeforeField) {
              // The Toff is entirely within this value.
              toffsList.add(toffs);
              toffsIterator.remove();
            } else if (startsAfterField) {
              /*
               * The Toffs starts within this value but ends after this value
               * so we clamp the returned Toffs to this value and leave the
               * Toffs in the iterator for the next value of this field.
               */
              toffsList.add(new Toffs(toffs.getStartOffset(), fieldEnd - 1));
            } else if (endsBeforeField) {
              /*
               * The Toffs starts before this value but ends in this value
               * which means we're really continuing from where we left off
               * above. Since we use the remainder of the offset we can remove
               * it from the iterator.
               */
              toffsList.add(new Toffs(fieldStart, toffs.getEndOffset()));
              toffsIterator.remove();
            } else {
              /*
               * The Toffs spans the whole value so we clamp on both sides.
               * This is basically a combination of both arms of the loop
               * above.
               */
              toffsList.add(new Toffs(fieldStart, fieldEnd - 1));
            }
          }
          if (!toffsList.isEmpty()) {
            subInfos.add(new SubInfo(subInfo.getText(), toffsList, subInfo.getSeqnum(), subInfo.getBoost()));
            boost += subInfo.getBoost();
          }

          if (subInfo.getTermsOffsets().isEmpty()) {
            subInfoIterator.remove();
          }
        }
        // RP-7664: fix a bug in case of multiValue highlighting
        WeightedFragInfo weightedFragInfo = new WeightedFragInfo(fragStart, fragEnd - 1, subInfos, boost);
        fieldNameToFragInfos.get(field.name()).add(weightedFragInfo);
      }
    }

    List<WeightedFragInfo> result = new ArrayList<>();
    for (List<WeightedFragInfo> weightedFragInfos : fieldNameToFragInfos.values()) {
      result.addAll(weightedFragInfos);
    }
    Collections.sort(result, new Comparator<WeightedFragInfo>() {

      @Override
      public int compare(FieldFragList.WeightedFragInfo info1, FieldFragList.WeightedFragInfo info2) {
        return info1.getStartOffset() - info2.getStartOffset();
      }

    });

    return result;
  }
}
