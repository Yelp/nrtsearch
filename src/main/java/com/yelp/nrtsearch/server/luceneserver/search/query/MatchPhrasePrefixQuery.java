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
package com.yelp.nrtsearch.server.luceneserver.search.query;

import static com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator.isAnalyzerDefined;

import com.carrotsearch.hppc.ObjectHashSet;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

/**
 * Lucene query to match all documents that contain terms in the same order as a provided query,
 * with the last term being treated as a prefix. The prefix is expanded to the first
 * (max_expansions) terms found in the index containing the prefix.
 *
 * <p>This class is a modified version of that used by <a
 * href="https://github.com/elastic/elasticsearch/blob/v7.2.0/server/src/main/java/org/elasticsearch/common/lucene/search/MultiPhrasePrefixQuery.java">
 * Elasticsearch</a>.
 */
public class MatchPhrasePrefixQuery extends Query {
  public static final int DEFAULT_MAX_EXPANSION = 50;

  private final String field;
  private final ArrayList<Term[]> termArrays;
  private final ArrayList<Integer> positions;
  private final int maxExpansions;
  private final int slop;

  /**
   * Build the lucene query based on the gRPC {@link
   * com.yelp.nrtsearch.server.grpc.MatchPhrasePrefixQuery} definition.
   *
   * @param matchPhrasePrefixQueryGrpc grpc query message
   * @param indexState index state
   * @return lucene query
   */
  public static Query build(
      com.yelp.nrtsearch.server.grpc.MatchPhrasePrefixQuery matchPhrasePrefixQueryGrpc,
      IndexState indexState) {
    FieldDef fieldDef = indexState.getField(matchPhrasePrefixQueryGrpc.getField());
    if (!(fieldDef instanceof IndexableFieldDef)) {
      throw new IllegalArgumentException("MatchPhrasePrefixQuery requires an indexable field");
    }
    IndexableFieldDef indexableFieldDef = (IndexableFieldDef) fieldDef;
    if (!indexableFieldDef.isSearchable()) {
      throw new IllegalArgumentException(
          "Field " + matchPhrasePrefixQueryGrpc.getField() + " is not searchable");
    }
    Analyzer analyzer =
        isAnalyzerDefined(matchPhrasePrefixQueryGrpc.getAnalyzer())
            ? AnalyzerCreator.getInstance().getAnalyzer(matchPhrasePrefixQueryGrpc.getAnalyzer())
            : indexState.searchAnalyzer;
    try (TokenStream stream =
        analyzer.tokenStream(
            matchPhrasePrefixQueryGrpc.getField(), matchPhrasePrefixQueryGrpc.getQuery())) {
      return createQueryFromTokenStream(
          stream,
          indexableFieldDef,
          matchPhrasePrefixQueryGrpc.getSlop(),
          matchPhrasePrefixQueryGrpc.getMaxExpansions());
    } catch (IOException e) {
      throw new RuntimeException("Error analyzing query", e);
    }
  }

  /**
   * Create match phrase prefix query for the given field using the analyzed token stream of the
   * input query text.
   *
   * @param stream token stream of query text
   * @param fieldDef field to match on
   * @param slop phrase matching slop
   * @param maxExpansions max terms to expand prefix into, or 0 for default (50)
   * @return lucene query
   * @throws IOException
   */
  public static Query createQueryFromTokenStream(
      TokenStream stream, IndexableFieldDef fieldDef, int slop, int maxExpansions)
      throws IOException {
    int resolvedMaxExpansions = maxExpansions > 0 ? maxExpansions : DEFAULT_MAX_EXPANSION;
    String field = fieldDef.getName();
    ArrayList<Term[]> termArrays = new ArrayList<>();
    ArrayList<Integer> positions = new ArrayList<>();

    List<Term> currentTerms = new ArrayList<>();
    TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
    PositionLengthAttribute posLenAtt = stream.addAttribute(PositionLengthAttribute.class);

    // no terms
    if (termAtt == null) {
      return new MatchNoDocsQuery();
    }

    stream.reset();
    int position = -1;
    while (stream.incrementToken()) {
      if (posIncrAtt.getPositionIncrement() != 0) {
        if (!currentTerms.isEmpty()) {
          termArrays.add(currentTerms.toArray(new Term[0]));
          positions.add(position);
        }
        position += posIncrAtt.getPositionIncrement();
        currentTerms.clear();
      }
      int positionLength = posLenAtt.getPositionLength();
      if (positionLength > 1) {
        throw new IllegalArgumentException(
            "MatchPhrasePrefixQuery does not support graph type analyzers");
      }
      currentTerms.add(new Term(field, termAtt.getBytesRef()));
    }
    // no tokens in query text
    if (position == -1) {
      return new MatchNoDocsQuery();
    }
    termArrays.add(currentTerms.toArray(new Term[0]));
    positions.add(position);

    // if more than one term, phrase matching is needed, requiring indexed positions
    if (termArrays.size() > 1) {
      IndexOptions indexOptions = fieldDef.getFieldType().indexOptions();
      if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
        throw new IllegalArgumentException(
            "MatchPhrasePrefixQuery field " + field + " not indexed with positions");
      }
    }

    return new MatchPhrasePrefixQuery(field, termArrays, positions, resolvedMaxExpansions, slop);
  }

  /**
   * Constructor.
   *
   * @param field field name
   * @param termArrays arrays of terms for each position
   * @param positions positions for each term group
   * @param maxExpansions max terms to expand prefix into
   * @param slop phrase matching slop
   */
  public MatchPhrasePrefixQuery(
      String field,
      ArrayList<Term[]> termArrays,
      ArrayList<Integer> positions,
      int maxExpansions,
      int slop) {
    this.field = field;
    this.termArrays = termArrays;
    this.positions = positions;
    this.maxExpansions = maxExpansions;
    this.slop = slop;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten = super.rewrite(reader);
    if (rewritten != this) {
      return rewritten;
    }
    if (termArrays.isEmpty()) {
      return new MatchNoDocsQuery();
    }
    MultiPhraseQuery.Builder query = new MultiPhraseQuery.Builder();
    query.setSlop(slop);
    int sizeMinus1 = termArrays.size() - 1;
    for (int i = 0; i < sizeMinus1; i++) {
      query.add(termArrays.get(i), positions.get(i));
    }
    Term[] suffixTerms = termArrays.get(sizeMinus1);
    int position = positions.get(sizeMinus1);
    ObjectHashSet<Term> terms = new ObjectHashSet<>();
    for (Term term : suffixTerms) {
      getPrefixTerms(terms, term, reader);
      if (terms.size() > maxExpansions) {
        break;
      }
    }
    if (terms.isEmpty()) {
      if (sizeMinus1 == 0) {
        // no prefix and the phrase query is empty
        return new MatchNoDocsQuery();
      }

      // if the terms does not exist we could return a MatchNoDocsQuery but this would break the
      // unified highlighter
      // which rewrites query with an empty reader.
      return new BooleanQuery.Builder()
          .add(query.build(), BooleanClause.Occur.MUST)
          .add(new MatchNoDocsQuery(), BooleanClause.Occur.MUST)
          .build();
    }
    query.add(terms.toArray(Term.class), position);
    return query.build();
  }

  private void getPrefixTerms(
      ObjectHashSet<Term> terms, final Term prefix, final IndexReader reader) throws IOException {
    // SlowCompositeReaderWrapper could be used... but this would merge all terms from each segment
    // into one terms
    // instance, which is very expensive. Therefore I think it is better to iterate over each leaf
    // individually.
    List<LeafReaderContext> leaves = reader.leaves();
    for (LeafReaderContext leaf : leaves) {
      Terms _terms = leaf.reader().terms(field);
      if (_terms == null) {
        continue;
      }

      TermsEnum termsEnum = _terms.iterator();
      TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(prefix.bytes());
      if (TermsEnum.SeekStatus.END == seekStatus) {
        continue;
      }

      for (BytesRef term = termsEnum.term(); term != null; term = termsEnum.next()) {
        if (!StringHelper.startsWith(term, prefix.bytes())) {
          break;
        }

        terms.add(new Term(field, BytesRef.deepCopyOf(term)));
        if (terms.size() >= maxExpansions) {
          return;
        }
      }
    }
  }

  @Override
  public final String toString(String f) {
    StringBuilder buffer = new StringBuilder();
    if (field.equals(f) == false) {
      buffer.append(field);
      buffer.append(":");
    }

    buffer.append("\"");
    Iterator<Term[]> i = termArrays.iterator();
    while (i.hasNext()) {
      Term[] terms = i.next();
      if (terms.length > 1) {
        buffer.append("(");
        for (int j = 0; j < terms.length; j++) {
          buffer.append(terms[j].text());
          if (j < terms.length - 1) {
            if (i.hasNext()) {
              buffer.append(" ");
            } else {
              buffer.append("* ");
            }
          }
        }
        if (i.hasNext()) {
          buffer.append(") ");
        } else {
          buffer.append("*)");
        }
      } else {
        buffer.append(terms[0].text());
        if (i.hasNext()) {
          buffer.append(" ");
        } else {
          buffer.append("*");
        }
      }
    }
    buffer.append("\"");

    if (slop != 0) {
      buffer.append("~");
      buffer.append(slop);
    }

    return buffer.toString();
  }

  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (sameClassAs(o) == false) {
      return false;
    }
    MatchPhrasePrefixQuery other = (MatchPhrasePrefixQuery) o;
    return this.slop == other.slop
        && termArraysEquals(this.termArrays, other.termArrays)
        && this.positions.equals(other.positions);
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    return classHash() ^ slop ^ termArraysHashCode() ^ positions.hashCode();
  }

  // Breakout calculation of the termArrays hashcode
  private int termArraysHashCode() {
    int hashCode = 1;
    for (final Term[] termArray : termArrays) {
      hashCode = 31 * hashCode + (termArray == null ? 0 : Arrays.hashCode(termArray));
    }
    return hashCode;
  }

  // Breakout calculation of the termArrays equals
  private boolean termArraysEquals(List<Term[]> termArrays1, List<Term[]> termArrays2) {
    if (termArrays1.size() != termArrays2.size()) {
      return false;
    }
    ListIterator<Term[]> iterator1 = termArrays1.listIterator();
    ListIterator<Term[]> iterator2 = termArrays2.listIterator();
    while (iterator1.hasNext()) {
      Term[] termArray1 = iterator1.next();
      Term[] termArray2 = iterator2.next();
      if (!(termArray1 == null ? termArray2 == null : Arrays.equals(termArray1, termArray2))) {
        return false;
      }
    }
    return true;
  }
}
