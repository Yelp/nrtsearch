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
package com.yelp.nrtsearch.server.luceneserver.highlights;

import static com.yelp.nrtsearch.server.luceneserver.highlights.HighlightUtils.adaptHighlightToV2;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.BoolValue;
import com.google.protobuf.UInt32Value;
import com.yelp.nrtsearch.server.grpc.Highlight;
import com.yelp.nrtsearch.server.grpc.Highlight.Settings;
import com.yelp.nrtsearch.server.grpc.Highlight.Type;
import com.yelp.nrtsearch.server.grpc.HighlightV2;
import com.yelp.nrtsearch.server.grpc.Query;
import java.util.List;
import org.junit.Test;

public class HighlightUtilsTest {

  @Test
  public void testAdaptHighlightToV2Basic() {
    Highlight highlightV1 =
        Highlight.newBuilder()
            .setSettings(
                Settings.newBuilder()
                    .setHighlighterType(Type.DEFAULT)
                    .setFragmentSize(UInt32Value.of(125))
                    .setMaxNumberOfFragments(UInt32Value.of(1)))
            .addAllFields(List.of("field1", "field2"))
            .build();

    HighlightV2 expectedHighlightV2 =
        HighlightV2.newBuilder()
            .setHighlighterType("fast-vector-highlighter")
            .setSettings(
                HighlightV2.Settings.newBuilder()
                    .setFragmentSize(UInt32Value.of(125))
                    .setMaxNumberOfFragments(UInt32Value.of(1))
                    .setScoreOrdered(BoolValue.of(true))
                    .setFieldMatch(BoolValue.of(false)))
            .addAllFields(List.of("field1", "field2"))
            .build();

    assertThat(adaptHighlightToV2(highlightV1)).isEqualTo(expectedHighlightV2);
  }

  @Test
  public void testAdaptHighlightToV2FullSettings() {
    Query q1 = Query.newBuilder().build(), q2 = Query.newBuilder().build();
    Highlight highlightV1 =
        Highlight.newBuilder()
            .setSettings(
                Settings.newBuilder()
                    .addPreTags("<start>")
                    .addPostTags("<end>")
                    .setHighlighterType(Type.DEFAULT)
                    .setFragmentSize(UInt32Value.of(125))
                    .setMaxNumberOfFragments(UInt32Value.of(1))
                    .setHighlightQuery(q1)
                    .setFieldMatch(true))
            .addAllFields(List.of("field1", "field2"))
            .putFieldSettings(
                "field2",
                Settings.newBuilder()
                    .addPreTags("*start*")
                    .addPostTags("*end*")
                    .setHighlighterType(Type.FAST_VECTOR)
                    .setFragmentSize(UInt32Value.of(80))
                    .setMaxNumberOfFragments(UInt32Value.of(3))
                    .setHighlightQuery(q2)
                    .setFieldMatch(false)
                    .build())
            .build();

    HighlightV2 expectedHighlightV2 =
        HighlightV2.newBuilder()
            .setHighlighterType("fast-vector-highlighter")
            .setSettings(
                HighlightV2.Settings.newBuilder()
                    .addPreTags("<start>")
                    .addPostTags("<end>")
                    .setFragmentSize(UInt32Value.of(125))
                    .setMaxNumberOfFragments(UInt32Value.of(1))
                    .setHighlightQuery(q1)
                    .setScoreOrdered(BoolValue.of(true))
                    .setFieldMatch(BoolValue.of(true)))
            .addAllFields(List.of("field1", "field2"))
            .putFieldSettings(
                "field2",
                HighlightV2.Settings.newBuilder()
                    .addPreTags("*start*")
                    .addPostTags("*end*")
                    .setFragmentSize(UInt32Value.of(80))
                    .setMaxNumberOfFragments(UInt32Value.of(3))
                    .setHighlightQuery(q2)
                    .setScoreOrdered(BoolValue.of(true))
                    .setFieldMatch(BoolValue.of(false))
                    .build())
            .build();

    assertThat(adaptHighlightToV2(highlightV1)).isEqualTo(expectedHighlightV2);
  }
}
