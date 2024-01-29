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
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.suggest.document.TopSuggestDocs;
import org.apache.lucene.search.suggest.document.TopSuggestDocsCollector;
import org.junit.Before;
import org.junit.Test;

public class MyTopSuggestDocsCollectorTest {
  TopSuggestDocs.SuggestScoreDoc[][] suggestScoreDocsByCollector;

  @Before
  public void setUp() throws IOException {
    suggestScoreDocsByCollector =
        new TopSuggestDocs.SuggestScoreDoc[][] {
          new TopSuggestDocs.SuggestScoreDoc[] {
            new TopSuggestDocs.SuggestScoreDoc(1, "Pizza Place", "a", 22),
            new TopSuggestDocs.SuggestScoreDoc(6, "Pizza", "a", 17),
            new TopSuggestDocs.SuggestScoreDoc(3, "Pizza", "a", 15),
            new TopSuggestDocs.SuggestScoreDoc(1, "Pizza", "b", 12),
            new TopSuggestDocs.SuggestScoreDoc(1, "Pizza", "a", 11),
          },
          new TopSuggestDocs.SuggestScoreDoc[] {
            new TopSuggestDocs.SuggestScoreDoc(2, "Pizza Palace", "a", 25),
            new TopSuggestDocs.SuggestScoreDoc(4, "Pizza", "a", 16),
            new TopSuggestDocs.SuggestScoreDoc(2, "Pizza", "b", 12),
            new TopSuggestDocs.SuggestScoreDoc(5, "Pizza", "a", 5),
            new TopSuggestDocs.SuggestScoreDoc(7, "Pizza", "a", 4),
          }
        };
  }

  /**
   * Tests the reduce method. We simulate two collectors, each containing a different array of
   * SuggestScoreDoc. The test is designed to have docs unsorted, with same docId withing a single
   * collector and between the two collectors.
   *
   * <p>The test asserts that the final list of suggest score docs is properly deduplicated, sorted
   * and limited using a predefined expected final array of suggest score docs.
   *
   * @throws IOException on error collecting results from collector
   */
  @Test
  public void shouldGetTopScoringSuggestScoreDocsUniqueById() throws IOException {
    TopSuggestDocs.SuggestScoreDoc[] expectedFinalSuggestScoreDocs = {
      new TopSuggestDocs.SuggestScoreDoc(2, "Pizza Palace", "a", 25),
      new TopSuggestDocs.SuggestScoreDoc(1, "Pizza Place", "a", 22),
      new TopSuggestDocs.SuggestScoreDoc(6, "Pizza", "a", 17),
      new TopSuggestDocs.SuggestScoreDoc(4, "Pizza", "a", 16),
      new TopSuggestDocs.SuggestScoreDoc(3, "Pizza", "a", 15),
    };

    MyTopSuggestDocsCollector.MyTopSuggestDocsCollectorManager manager =
        new MyTopSuggestDocsCollector.MyTopSuggestDocsCollectorManager(5);

    Collection<TopSuggestDocsCollector> collectors =
        this.createCollectorsAndCollectDocuments(manager);

    TopSuggestDocs reduced = manager.reduce(collectors);

    // check total hits
    assertEquals(5, reduced.totalHits.value);
    assertEquals(TotalHits.Relation.EQUAL_TO, reduced.totalHits.relation);

    // check suggest score docs
    assertEquals(expectedFinalSuggestScoreDocs.length, reduced.scoreDocs.length);
    for (int i = 0; i < expectedFinalSuggestScoreDocs.length; i++) {
      TopSuggestDocs.SuggestScoreDoc expected = expectedFinalSuggestScoreDocs[i];
      TopSuggestDocs.SuggestScoreDoc actual = reduced.scoreLookupDocs()[i];
      assertEquals(expected.score, actual.score, 0.001);
      assertEquals(expected.doc, actual.doc);
      assertEquals(expected.context, actual.context);
    }
  }

  /**
   * This test asserts the exhaustive behaviour of collectors. We expect collectors to keep
   * collecting new items, even if they have lower scores than the lowest score in their queue as we
   * can not guarantee that suggestScoreDocs collected are sorted by highest score.
   *
   * <p>The manager created has `numHitsToCollect = 2`, which would fill `collector2`'s queue, then
   * pass a document with low score and then again some with higher scores.
   *
   * @throws IOException on error collecting results from collector
   */
  @Test
  public void
      shouldGetTopScoringSuggestScoreDocsUniqueByIdWithUnorderedSuggestDocsOverflowingCollectorQueue()
          throws IOException {
    TopSuggestDocs.SuggestScoreDoc[] expectedFinalSuggestScoreDocs = {
      new TopSuggestDocs.SuggestScoreDoc(2, "Pizza Palace", "a", 25),
      new TopSuggestDocs.SuggestScoreDoc(1, "Pizza Place", "a", 22)
    };

    MyTopSuggestDocsCollector.MyTopSuggestDocsCollectorManager manager =
        new MyTopSuggestDocsCollector.MyTopSuggestDocsCollectorManager(2);

    Collection<TopSuggestDocsCollector> collectors =
        this.createCollectorsAndCollectDocuments(manager);
    TopSuggestDocs reduced = manager.reduce(collectors);

    // check total hits
    assertEquals(2, reduced.totalHits.value);
    assertEquals(TotalHits.Relation.EQUAL_TO, reduced.totalHits.relation);

    // check suggest score docs
    assertEquals(expectedFinalSuggestScoreDocs.length, reduced.scoreDocs.length);
    for (int i = 0; i < expectedFinalSuggestScoreDocs.length; i++) {
      TopSuggestDocs.SuggestScoreDoc expected = expectedFinalSuggestScoreDocs[i];
      TopSuggestDocs.SuggestScoreDoc actual = reduced.scoreLookupDocs()[i];
      assertEquals(expected.score, actual.score, 0.001);
      assertEquals(expected.doc, actual.doc);
      assertEquals(expected.context, actual.context);
    }
  }

  private Collection<TopSuggestDocsCollector> createCollectorsAndCollectDocuments(
      MyTopSuggestDocsCollector.MyTopSuggestDocsCollectorManager manager) throws IOException {
    List<TopSuggestDocsCollector> collectors = new LinkedList<>();
    for (TopSuggestDocs.SuggestScoreDoc[] suggestScoreDocsForCollector :
        suggestScoreDocsByCollector) {
      TopSuggestDocsCollector topSuggestDocsCollector = manager.newCollector();
      for (TopSuggestDocs.SuggestScoreDoc suggestScoreDoc : suggestScoreDocsForCollector) {
        try {
          topSuggestDocsCollector.collect(
              suggestScoreDoc.doc,
              suggestScoreDoc.key,
              suggestScoreDoc.context,
              suggestScoreDoc.score);
        } catch (CollectionTerminatedException e) {
          // simulate break on collection terminated
          break;
        }
      }
      collectors.add(topSuggestDocsCollector);
    }
    return collectors;
  }
}
