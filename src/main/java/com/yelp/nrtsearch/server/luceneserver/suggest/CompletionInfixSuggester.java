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
package com.yelp.nrtsearch.server.luceneserver.suggest;

import com.yelp.nrtsearch.server.luceneserver.suggest.iterator.SuggestInputIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.search.suggest.document.Completion90PostingsFormat;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.ContextQuery;
import org.apache.lucene.search.suggest.document.ContextSuggestField;
import org.apache.lucene.search.suggest.document.PrefixCompletionQuery;
import org.apache.lucene.search.suggest.document.SuggestIndexSearcher;
import org.apache.lucene.search.suggest.document.TopSuggestDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/**
 * Analyzes the input text and suggests matched suggest items based on the prefix matches to any
 * pre-analyzed suffix-token gram in the indexed text.
 *
 * <p>This suggester relies on a customized InputIterator FromFileSuggestItemIterator to build
 * suggestion index. Each suggest text is pre-analyzed as a list of suffix-token grams.
 *
 * <p>This suggester requires payload defined for each suggest item. The payload persist necessary
 * information for the downstream services.
 *
 * <p>This suggester supports multiple contexts. The suggested results are returned only when any of
 * its contexts matches any context in the lookup query. Context match is not considered if there is
 * no context specified in the lookup query.
 *
 * <p>The same as AnalyzingInfixSuggester, results are sorted by descending weight values.
 *
 * <p>Example usage of this suggester: 1. Use FromFileSuggestItemIterator iterator to read the raw
 * file of suggest items 2. call build method with FromFileSuggestItemIterator iterator to build
 * indexes 3. call lookup method to look up the matched items based on the input text and optional
 * contexts.
 */
public class CompletionInfixSuggester extends AnalyzingInfixSuggester {

  private static final String EXACT_TEXT_FIELD_NAME = "text";
  protected static final String SEARCH_TEXT_FIELD_NAME = "search_text";
  private static final String PAYLOAD_FIELD_NAME = "payload";
  private static final boolean DEFAULT_COMMIT_ON_BUILD = false;
  private static final boolean DEFAULT_SKIP_DUPLICATION = true;

  private final Directory dir;

  public CompletionInfixSuggester(Directory dir, Analyzer indexAnalyzer, Analyzer queryAnalyzer)
      throws IOException {
    super(dir, indexAnalyzer, queryAnalyzer, DEFAULT_MIN_PREFIX_CHARS, DEFAULT_COMMIT_ON_BUILD);
    this.dir = dir;
  }

  @Override
  public List<LookupResult> lookup(
      CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num)
      throws IOException, RuntimeException {
    if (searcherMgr == null) {
      throw new RuntimeException("No valid searcher manager available!");
    }

    IndexSearcher searcher;
    SearcherManager mgr;
    List<LookupResult> results;

    synchronized (searcherMgrLock) {
      mgr = searcherMgr; // acquire & release on same SearcherManager, via local reference
      searcher = searcherMgr.acquire();
    }

    try {
      SuggestIndexSearcher suggestIndexSearcher =
          new SuggestIndexSearcher(searcher.getIndexReader());
      ContextQuery finalQuery = createContextQuery(key, contexts);
      TopSuggestDocs topDocs =
          suggestIndexSearcher.suggest(finalQuery, num, DEFAULT_SKIP_DUPLICATION);
      results = createResults(suggestIndexSearcher, topDocs, contexts);
    } finally {
      mgr.release(searcher);
    }

    return results;
  }

  protected List<LookupResult> createResults(
      SuggestIndexSearcher suggestIndexSearcher, TopSuggestDocs topDocs, Set<BytesRef> contexts)
      throws IOException {
    if (topDocs.scoreDocs == null || topDocs.scoreDocs.length == 0) {
      return List.of();
    }

    List<LookupResult> results = new ArrayList<>();
    Set<Integer> visitedDocIds = new HashSet<>();
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      if (visitedDocIds.contains(scoreDoc.doc)) {
        continue;
      }
      visitedDocIds.add(scoreDoc.doc);

      Document curDoc = suggestIndexSearcher.doc(scoreDoc.doc);
      IndexableField textField = curDoc.getField(EXACT_TEXT_FIELD_NAME);
      assert textField != null : EXACT_TEXT_FIELD_NAME + " field can not be null";
      IndexableField payloadField = curDoc.getField(PAYLOAD_FIELD_NAME);
      assert payloadField != null : PAYLOAD_FIELD_NAME + " field can not be null";

      results.add(
          new LookupResult(
              textField.stringValue(),
              (long) scoreDoc.score,
              payloadField.binaryValue(),
              contexts));
    }

    return results;
  }

  private ContextQuery createContextQuery(CharSequence key, Set<BytesRef> contexts) {
    CompletionQuery completionQuery = createCompletionQuery(key);
    ContextQuery contextQuery = new ContextQuery(completionQuery);

    if (contexts != null) {
      for (BytesRef context : contexts) {
        contextQuery.addContext(context.utf8ToString());
      }
    }

    return contextQuery;
  }

  protected CompletionQuery createCompletionQuery(CharSequence key) {
    return new PrefixCompletionQuery(
        queryAnalyzer, new Term(SEARCH_TEXT_FIELD_NAME, new BytesRef(key)));
  }

  @Override
  public void build(InputIterator iterator) throws IOException {
    if (!(iterator instanceof SuggestInputIterator)) {
      throw new IllegalArgumentException(
          "this suggester only works with iterator that implementing SuggestInputIterator");
    }

    SuggestInputIterator iter = (SuggestInputIterator) iterator;
    if (!iter.hasPayloads()) {
      throw new IllegalArgumentException("this suggester requires to have payload in index");
    }
    if (!iter.hasContexts()) {
      throw new IllegalArgumentException("this suggester requires to have context in index");
    }
    if (!iter.hasSearchTexts()) {
      throw new IllegalArgumentException("this suggester requires to have search texts in index");
    }

    synchronized (searcherMgrLock) {
      if (searcherMgr != null) {
        searcherMgr.close();
        searcherMgr = null;
      }

      if (writer != null) {
        writer.close();
        writer = null;
      }

      boolean success = false;
      try {
        writer =
            new IndexWriter(
                dir, getIndexWriterConfig(indexAnalyzer, IndexWriterConfig.OpenMode.CREATE));

        BytesRef text;
        while ((text = iterator.next()) != null) {
          add(text, iter.searchTexts(), iter.contexts(), iter.weight(), iter.payload());
        }

        searcherMgr = new SearcherManager(writer, null);
        success = true;
      } finally {
        if (success) {
          writer.close();
          writer = null;
        } else {
          if (writer != null) {
            writer.rollback();
            writer = null;
          }
        }
      }
    }
  }

  public void update(
      BytesRef text,
      Set<BytesRef> searchTexts,
      Set<BytesRef> contexts,
      long weight,
      BytesRef payload)
      throws IOException {
    ensureOpen();
    writer.updateDocument(
        new Term(EXACT_TEXT_FIELD_NAME, text.utf8ToString()),
        buildDocument(text, searchTexts, contexts, weight, payload));
  }

  public void add(
      BytesRef text,
      Set<BytesRef> searchText,
      Set<BytesRef> contexts,
      long weight,
      BytesRef payload)
      throws IOException {
    ensureOpen();
    writer.addDocument(buildDocument(text, searchText, contexts, weight, payload));
  }

  protected IndexWriterConfig getIndexWriterConfig(
      Analyzer indexAnalyzer, IndexWriterConfig.OpenMode mode) {
    IndexWriterConfig iwc = super.getIndexWriterConfig(indexAnalyzer, mode);
    Codec filterCodec =
        new Lucene95Codec() {
          final PostingsFormat fstPostingsFormat = new Completion90PostingsFormat();

          @Override
          public PostingsFormat getPostingsFormatForField(String field) {
            if (SEARCH_TEXT_FIELD_NAME.equals(field)) {
              return fstPostingsFormat;
            }
            return super.getPostingsFormatForField(field);
          }
        };
    iwc.setCodec(filterCodec);
    return iwc;
  }

  private Document buildDocument(
      BytesRef text,
      Set<BytesRef> searchTexts,
      Set<BytesRef> contexts,
      long weight,
      BytesRef payload) {
    Document doc = new Document();

    doc.add(new StoredField(PAYLOAD_FIELD_NAME, payload));
    doc.add(new StoredField(EXACT_TEXT_FIELD_NAME, text.utf8ToString()));

    int weightInt = (int) weight;
    CharSequence[] contextSequence = convertSetToCharSeq(contexts);
    for (BytesRef searchText : searchTexts) {
      doc.add(
          new ContextSuggestField(
              SEARCH_TEXT_FIELD_NAME, searchText.utf8ToString(), weightInt, contextSequence));
    }
    return doc;
  }

  private CharSequence[] convertSetToCharSeq(Set<BytesRef> bytesRefSet) {
    if (bytesRefSet == null || bytesRefSet.size() == 0) {
      return new CharSequence[0];
    }

    CharSequence[] charSeq = new CharSequence[bytesRefSet.size()];
    int index = 0;
    for (BytesRef text : bytesRefSet) {
      charSeq[index++] = text.utf8ToString();
    }
    return charSeq;
  }

  /** Copy from AnalyzingInfixSuggester Todo: add license acknowledge? */
  protected synchronized void ensureOpen() throws IOException {
    if (writer == null) {
      if (DirectoryReader.indexExists(dir)) {
        // Already built; open it:
        writer =
            new IndexWriter(
                dir, getIndexWriterConfig(indexAnalyzer, IndexWriterConfig.OpenMode.APPEND));
      } else {
        writer =
            new IndexWriter(
                dir, getIndexWriterConfig(indexAnalyzer, IndexWriterConfig.OpenMode.CREATE));
      }
      synchronized (searcherMgrLock) {
        SearcherManager oldSearcherMgr = searcherMgr;
        searcherMgr = new SearcherManager(writer, null);
        if (oldSearcherMgr != null) {
          oldSearcherMgr.close();
        }
      }
    }
  }
}
