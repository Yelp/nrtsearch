Match Query
==========================

A query that analyzes the text before finding matching documents. The tokens resulting from the analysis are combined using Lucene TermQuery or FuzzyQuery in BooleanClauses. This is similar to Match query in Elasticsearch.

Proto definition:

.. code-block::

   message MatchQuery {
       string field = 1; // Field in the document to query.
       string query = 2; // The text to query with.
       MatchOperator operator = 3; // Boolean logic used to interpret text in the query. The possible values are SHOULD (default) and MUST.
       int32 minimumNumberShouldMatch = 4; // Minimum number of optional clauses that must match.
       Analyzer analyzer = 5; // Analyzer used to analyze the query. If not provided, the default search analyzer for the field would be used instead.
       FuzzyParams fuzzyParams = 6; // Parameters to set the fuzziness of the query
   }

   message FuzzyParams {
       int32 maxEdits = 1; // The maximum allowed Levenshtein Edit Distance (or number of edits). Possible values are 0, 1 and 2.
       int32 prefixLength = 2; // Length of common (non-fuzzy) prefix
       int32 maxExpansions = 3; // The maximum number of terms to match.
       bool transpositions = 4; // True if transpositions should be treated as a primitive edit operation. If this is false (default), comparisons will implement the classic Levenshtein algorithm.
   }

   enum MatchOperator {
       SHOULD = 0; // At least one term must match the document
       MUST = 1; // All of the terms must match the document
   }