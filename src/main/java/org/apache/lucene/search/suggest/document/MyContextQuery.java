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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.lucene.analysis.miscellaneous.ConcatenateGraphFilter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.fst.Util;

/**
 * Modified version of lucene {@link ContextQuery}. In toContextAutomaton(), the context FST is
 * built by creating the {@link Automaton} for each context, then creating the union. The original
 * query unions each context together individually, which is not efficient. Unfortunately, because
 * of the use of private members/methods, most of the original class needed to be copied.
 */
public class MyContextQuery extends ContextQuery {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(MyContextQuery.class);

  private IntsRefBuilder scratch = new IntsRefBuilder();
  private Map<IntsRef, ContextMetaData> contexts;
  private boolean matchAllContexts = false;
  /** Inner completion query */
  protected CompletionQuery innerQuery;

  private long ramBytesUsed;

  /**
   * Constructs a context completion query that matches documents specified by <code>query</code>.
   *
   * <p>Use {@link #addContext(CharSequence, float, boolean)} to add context(s) with boost
   */
  public MyContextQuery(CompletionQuery query) {
    super(query);
    if (query instanceof ContextQuery) {
      throw new IllegalArgumentException(
          "'query' parameter must not be of type " + this.getClass().getSimpleName());
    }
    this.innerQuery = query;
    contexts = new HashMap<>();
    updateRamBytesUsed();
  }

  private void updateRamBytesUsed() {
    ramBytesUsed =
        BASE_RAM_BYTES
            + RamUsageEstimator.sizeOfObject(contexts)
            + RamUsageEstimator.sizeOfObject(
                innerQuery, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED);
  }

  /** Adds an exact context with default boost of 1 */
  public void addContext(CharSequence context) {
    addContext(context, 1f, true);
  }

  /** Adds an exact context with boost */
  public void addContext(CharSequence context, float boost) {
    addContext(context, boost, true);
  }

  /**
   * Adds a context with boost, set <code>exact</code> to false if the context is a prefix of any
   * indexed contexts
   */
  public void addContext(CharSequence context, float boost, boolean exact) {
    addContextInternal(context, boost, exact);
    updateRamBytesUsed();
  }

  /**
   * Add a List of exact contexts with default boost of 1.
   *
   * @param contextsList contexts
   * @param <T> context type
   */
  public <T extends CharSequence> void addContexts(List<T> contextsList) {
    for (CharSequence context : contextsList) {
      addContextInternal(context, 1.0f, true);
    }
    // updating memory usage scales with number of contexts, so only do it once
    updateRamBytesUsed();
  }

  private void addContextInternal(CharSequence context, float boost, boolean exact) {
    if (boost < 0f) {
      throw new IllegalArgumentException("'boost' must be >= 0");
    }
    for (int i = 0; i < context.length(); i++) {
      if (ContextSuggestField.CONTEXT_SEPARATOR == context.charAt(i)) {
        throw new IllegalArgumentException(
            "Illegal value ["
                + context
                + "] UTF-16 codepoint [0x"
                + Integer.toHexString((int) context.charAt(i))
                + "] at position "
                + i
                + " is a reserved character");
      }
    }
    contexts.put(
        IntsRef.deepCopyOf(Util.toIntsRef(new BytesRef(context), scratch)),
        new ContextMetaData(boost, exact));
  }

  /** Add all contexts with a boost of 1f */
  public void addAllContexts() {
    matchAllContexts = true;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    BytesRefBuilder scratch = new BytesRefBuilder();
    for (Map.Entry<IntsRef, ContextMetaData> entry : contexts.entrySet()) {
      if (buffer.length() != 0) {
        buffer.append(",");
      } else {
        buffer.append("contexts");
        buffer.append(":[");
      }
      buffer.append(Util.toBytesRef(entry.getKey(), scratch).utf8ToString());
      ContextMetaData metaData = entry.getValue();
      if (metaData.exact == false) {
        buffer.append("*");
      }
      if (metaData.boost != 0) {
        buffer.append("^");
        buffer.append(Float.toString(metaData.boost));
      }
    }
    if (buffer.length() != 0) {
      buffer.append("]");
      buffer.append(",");
    }
    return buffer.toString() + innerQuery.toString(field);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    final CompletionWeight innerWeight =
        ((CompletionWeight) innerQuery.createWeight(searcher, scoreMode, boost));
    final Automaton innerAutomaton = innerWeight.getAutomaton();

    // If the inner automaton matches nothing, then we return an empty weight to avoid
    // traversing all contexts during scoring.
    if (innerAutomaton.getNumStates() == 0) {
      return new CompletionWeight(this, innerAutomaton);
    }

    // if separators are preserved the fst contains a SEP_LABEL
    // behind each gap. To have a matching automaton, we need to
    // include the SEP_LABEL in the query as well
    Automaton optionalSepLabel =
        Operations.optional(Automata.makeChar(ConcatenateGraphFilter.SEP_LABEL));
    Automaton prefixAutomaton = Operations.concatenate(optionalSepLabel, innerAutomaton);
    Automaton contextsAutomaton =
        Operations.concatenate(toContextAutomaton(contexts, matchAllContexts), prefixAutomaton);
    contextsAutomaton =
        Operations.determinize(contextsAutomaton, Operations.DEFAULT_MAX_DETERMINIZED_STATES);

    final Map<IntsRef, Float> contextMap = new HashMap<>(contexts.size());
    final TreeSet<Integer> contextLengths = new TreeSet<>();
    for (Map.Entry<IntsRef, ContextMetaData> entry : contexts.entrySet()) {
      ContextMetaData contextMetaData = entry.getValue();
      contextMap.put(entry.getKey(), contextMetaData.boost);
      contextLengths.add(entry.getKey().length);
    }
    int[] contextLengthArray = new int[contextLengths.size()];
    final Iterator<Integer> iterator = contextLengths.descendingIterator();
    for (int i = 0; iterator.hasNext(); i++) {
      contextLengthArray[i] = iterator.next();
    }
    return new ContextCompletionWeight(
        this, contextsAutomaton, innerWeight, contextMap, contextLengthArray);
  }

  private static Automaton toContextAutomaton(
      final Map<IntsRef, ContextMetaData> contexts, final boolean matchAllContexts) {
    final Automaton matchAllAutomaton = Operations.repeat(Automata.makeAnyString());
    final Automaton sep = Automata.makeChar(ContextSuggestField.CONTEXT_SEPARATOR);
    if (matchAllContexts || contexts.size() == 0) {
      return Operations.concatenate(matchAllAutomaton, sep);
    } else {
      List<Automaton> contextAutos = new ArrayList<>(contexts.size());
      for (Map.Entry<IntsRef, ContextMetaData> entry : contexts.entrySet()) {
        final ContextMetaData contextMetaData = entry.getValue();
        final IntsRef ref = entry.getKey();
        Automaton contextAutomaton = Automata.makeString(ref.ints, ref.offset, ref.length);
        if (contextMetaData.exact == false) {
          contextAutomaton = Operations.concatenate(contextAutomaton, matchAllAutomaton);
        }
        contextAutos.add(Operations.concatenate(contextAutomaton, sep));
      }
      return Operations.union(contextAutos);
    }
  }

  /** Holder for context value meta data */
  private static class ContextMetaData {

    /** Boost associated with a context value */
    private final float boost;

    /**
     * flag to indicate whether the context value should be treated as an exact value or a context
     * prefix
     */
    private final boolean exact;

    private ContextMetaData(float boost, boolean exact) {
      this.boost = boost;
      this.exact = exact;
    }
  }

  private static class ContextCompletionWeight extends CompletionWeight {

    private final Map<IntsRef, Float> contextMap;
    private final int[] contextLengths;
    private final CompletionWeight innerWeight;
    private final BytesRefBuilder scratch = new BytesRefBuilder();

    private float currentBoost;
    private CharSequence currentContext;

    public ContextCompletionWeight(
        CompletionQuery query,
        Automaton automaton,
        CompletionWeight innerWeight,
        Map<IntsRef, Float> contextMap,
        int[] contextLengths)
        throws IOException {
      super(query, automaton);
      this.contextMap = contextMap;
      this.contextLengths = contextLengths;
      this.innerWeight = innerWeight;
    }

    @Override
    protected void setNextMatch(final IntsRef pathPrefix) {
      IntsRef ref = pathPrefix.clone();

      // check if the pathPrefix matches any
      // defined context, longer context first
      for (int contextLength : contextLengths) {
        if (contextLength > pathPrefix.length) {
          continue;
        }
        ref.length = contextLength;
        if (contextMap.containsKey(ref)) {
          currentBoost = contextMap.get(ref);
          ref.length = pathPrefix.length;
          setInnerWeight(ref, contextLength);
          return;
        }
      }
      // unknown context
      ref.length = pathPrefix.length;
      currentBoost = 0f;
      setInnerWeight(ref, 0);
    }

    private void setInnerWeight(IntsRef ref, int offset) {
      IntsRefBuilder refBuilder = new IntsRefBuilder();
      for (int i = offset; i < ref.length; i++) {
        if (ref.ints[ref.offset + i] == ContextSuggestField.CONTEXT_SEPARATOR) {
          if (i > 0) {
            refBuilder.copyInts(ref.ints, ref.offset, i);
            currentContext = Util.toBytesRef(refBuilder.get(), scratch).utf8ToString();
          } else {
            currentContext = null;
          }
          ref.offset = ++i;
          assert ref.offset < ref.length : "input should not end with the context separator";
          if (ref.ints[i] == ConcatenateGraphFilter.SEP_LABEL) {
            ref.offset++;
            assert ref.offset < ref.length
                : "input should not end with a context separator followed by SEP_LABEL";
          }
          ref.length = ref.length - ref.offset;
          refBuilder.copyInts(ref.ints, ref.offset, ref.length);
          innerWeight.setNextMatch(refBuilder.get());
          return;
        }
      }
    }

    @Override
    protected CharSequence context() {
      return currentContext;
    }

    @Override
    protected float boost() {
      return currentBoost + innerWeight.boost();
    }
  }

  @Override
  public boolean equals(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }
}
