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
package com.yelp.nrtsearch.server.luceneserver.warming;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.List;
import org.assertj.core.data.Percentage;
import org.junit.Test;

public class ReservoirSamplerTest {

  @Test
  public void testSampleQueryReservoirNotFull() {
    ReservoirSampler reservoirSampler = new ReservoirSampler(2);
    for (String index : List.of("index1", "index2")) {
      for (int i = 0; i < 2; i++) {
        ReservoirSampler.SampleResult sampleResult = reservoirSampler.sample(index);
        assertThat(sampleResult.isSample()).isTrue();
        assertThat(sampleResult.getReplace()).isEqualTo(i);
      }
    }
  }

  @Test
  public void testSampleQueryAfterReservoirFull() {
    ReservoirSampler reservoirSampler = new ReservoirSampler(2);
    Multimap<Boolean, Integer> sampleResults = ArrayListMultimap.create();
    for (int i = 0; i < 1000; i++) {
      ReservoirSampler.SampleResult sampleResult = reservoirSampler.sample("test_index");
      sampleResults.put(sampleResult.isSample(), sampleResult.getReplace());
    }
    assertThat(sampleResults.get(true)).containsOnly(0, 1);
    assertThat(sampleResults.get(false).size()).isCloseTo(900, Percentage.withPercentage(10));
  }
}
