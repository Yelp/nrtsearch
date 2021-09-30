Disjunction Max Query
==========================

A query that generates the union of documents produced by its subqueries, and that scores each document with the maximum score for that document as produced by any subquery, plus a tie breaking increment for any additional matching subqueries. This is useful when searching for a word in multiple fields with different boost factors (so that the fields cannot be combined equivalently into a single search field). This maps to DisjunctionMaxQuery in Lucene.

Proto definition:

.. code-block::

   message DisjunctionMaxQuery {
       repeated Query disjuncts = 1; // A list of all the disjuncts to add
       float tieBreakerMultiplier = 2; // The score of each non-maximum disjunct for a document is multiplied by this weight and added into the final score.
   }