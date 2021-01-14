package com.yelp.nrtsearch.server.luceneserver.suggest.iterator;

import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.util.BytesRef;

import java.util.Set;

/**
 * Interface is the child of InputIterator interface with extra support for search texts.
 * currently only CompletionInfixSuggest adopts the iterator implementing this interface.
 */
public interface SuggestInputIterator extends InputIterator {
    boolean hasSearchTexts();

    /**
     * A term's search texts are used to perform infix suggest for both exact matching and fuzzy match.
     * This field is critical in CompletionInfixSuggest, so that it can not be empty.
     */
    Set<BytesRef> searchTexts();
}
