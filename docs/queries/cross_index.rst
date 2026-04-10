Cross Index Query
==========================

A query that filters the primary index to documents whose join key matches documents in a secondary index. Uses Lucene's ``JoinUtil`` internally. The ``query`` runs on the secondary index; matching values of ``secondary_field`` are collected eagerly and used to build a TermsQuery on ``primary_field`` in the primary index. The secondary index searcher is acquired only during query construction and released immediately afterward — it is **not** held open during primary search execution.

Both ``primary_field`` and ``secondary_field`` must exist in their respective indices. The secondary index must be on the same nrtsearch node. The ``query`` field is required. If any of these are missing or invalid, an error is returned.

Nested cross-index queries are supported: the inner ``query`` may itself contain a ``CrossIndexQuery`` referencing a third index.

The ``score_mode`` controls how scores from matching secondary documents are aggregated and propagated to the primary index results:

  * ``JOIN_SCORE_NONE`` - No scoring; acts as a pure filter (default)

  * ``JOIN_SCORE_AVG`` - Average score of matching secondary documents

  * ``JOIN_SCORE_MAX`` - Maximum score of matching secondary documents

  * ``JOIN_SCORE_MIN`` - Minimum score of matching secondary documents

  * ``JOIN_SCORE_TOTAL`` - Sum of scores of matching secondary documents

When used inside a BooleanQuery ``FILTER`` clause, the cross-index query filters without contributing to the score regardless of ``score_mode``.

Memory considerations
---------------------

``JoinUtil`` collects all unique join-key values from matching secondary documents into an in-memory hash set. For large secondary result sets this can consume significant memory. Use the ``max_terms`` field to set an upper bound on the number of matching secondary documents allowed. If the inner query matches more documents than this limit, an error is returned instead of risking excessive memory usage.

Multi-valued join fields
------------------------

If the secondary join field is registered with ``multiValued: true``, this is automatically detected from the field definition and handled correctly by ``JoinUtil``. No additional configuration is needed.

Proto definition:

.. code-block::

   message CrossIndexQuery {
       enum JoinScoreMode {
           JOIN_SCORE_UNSET = 0; // Unset — treated as JOIN_SCORE_NONE (filter only)
           JOIN_SCORE_NONE = 1;  // No scoring - filter only
           JOIN_SCORE_AVG = 2;   // Average score of matching secondary documents
           JOIN_SCORE_MAX = 3;   // Maximum score of matching secondary documents
           JOIN_SCORE_MIN = 4;   // Minimum score of matching secondary documents
           JOIN_SCORE_TOTAL = 5; // Sum of scores of matching secondary documents
       }

       string index = 1;           // Name of the secondary index (must be on the same nrtsearch node)
       string primary_field = 2;   // Field in the primary index containing the join key
       string secondary_field = 3; // Field in the secondary index containing the join key
       Query query = 4;            // Query to execute on the secondary index (required)
       JoinScoreMode score_mode = 5; // Score aggregation mode (default: JOIN_SCORE_NONE)
       uint32 max_terms = 6;       // Max matching secondary docs allowed; 0 = unlimited (default: 0)
   }
