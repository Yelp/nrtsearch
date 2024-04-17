Match Query
==========================

A query that analyzes the text before finding matching documents. The tokens resulting from the analysis are combined using Lucene TermQuery or FuzzyQuery in BooleanClauses. This is similar to Match query in Elasticsearch.

Fuzziness: Allows inexact fuzzy matching using FuzzyParams. When querying text or keyword fields, fuzziness is interpreted as Levenshtein Edit Distance (the number of one character changes that need to be made to one string to make it the same as another string).

The FuzzyParams can be specified as:

maxEdits - 0, 1, 2 which is the maximum allowed Levenshtein Edit Distance.
AutoFuzziness - edit distance is computed based on the term length. It can optionally take a low and high value which are 3 and 6 by default.
1. [0, low) - Must match exactly
2. [low, high) - One edit allowed
3. >= high - Two edits allowed

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
        int32 prefixLength = 1; // Length of common (non-fuzzy) prefix
        int32 maxExpansions = 2; // The maximum number of terms to match.
        bool transpositions = 3; // True if transpositions should be treated as a primitive edit operation. If this is false (default), comparisons will implement the classic Levenshtein algorithm.

        // Fuzziness can be AUTO or based on number of edits. AUTO will determine maxEdits based in term length.
        oneof Fuzziness {
            int32 maxEdits = 4; // The maximum allowed Levenshtein Edit Distance (or number of edits). Possible values are 0, 1 and 2.
            AutoFuzziness auto = 5; // Auto fuzziness which determines the max edits based on the term length. AUTO is the preferred setting.
        }

        // Optional low and high values for auto fuzziness. Defaults to low: 3 and high: 6 if both are unset. Valid values are low >= 0 and low < high
        message AutoFuzziness {
            int32 low = 6; // Optional low distance argument.
            int32 high = 7; // Optional high distance argument.
        }
    }

   enum MatchOperator {
       SHOULD = 0; // At least one term must match the document
       MUST = 1; // All of the terms must match the document
   }