Phrase Query
==========================

A Query that matches documents containing a particular sequence of terms. It maps to Lucene's PhraseQuery.

Proto definition:

.. code-block::

   message PhraseQuery {
       /* Edit distance between respective positions of terms as defined in this PhraseQuery and the positions
          of terms in a document.
       */
       int32 slop = 1;
       string field = 2; // The field in the index that this query applies to.
       repeated string terms = 3; // Terms to match.
   }