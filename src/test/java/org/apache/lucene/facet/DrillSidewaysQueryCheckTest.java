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
package org.apache.lucene.facet;

import static org.apache.lucene.facet.DrillSidewaysQueryCheck.isDrillSidewaysQuery;
import static org.junit.Assert.*;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;
import org.mockito.Mockito;

public class DrillSidewaysQueryCheckTest {

  @Test
  public void testIsDrillSidewaysQuery() {
    assertFalse(isDrillSidewaysQuery(null));

    assertFalse(isDrillSidewaysQuery(new TermQuery(new Term("field", "text"))));

    DrillSidewaysQuery dsq = Mockito.mock(DrillSidewaysQuery.class);
    assertTrue(isDrillSidewaysQuery(dsq));
  }
}
