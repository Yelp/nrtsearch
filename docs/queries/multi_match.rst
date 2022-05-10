Multi-Match Query
==========================

A query that creates a match query for each field provided and wraps all the match queries in a DisjunctionMaxQuery.

Proto definition:

.. code-block::

   message MultiMatchQuery {
       repeated string fields = 1; // Fields in the document to query.
       string query = 2; // The text to query with.
       map<string, float> fieldBoosts = 3; // Boosts for each field, if any.
       MatchOperator operator = 4; // Boolean logic used to interpret text in the query. The possible values are SHOULD (default) and MUST.
       int32 minimumNumberShouldMatch = 5; // Minimum number of optional clauses that must match.
       Analyzer analyzer = 6; // Analyzer used to analyze the query. If not provided, the default search analyzer for the field would be used instead.
       FuzzyParams fuzzyParams = 7; // Parameters to set the fuzziness of the query
       float tieBreakerMultiplier = 8; // The score of each non-maximum match query disjunct for a document will be multiplied by this weight and added into the final score.
   }