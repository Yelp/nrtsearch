Min Score Query
==========================

A query wrapper that filters documents based on a minimum score threshold. Only documents with
scores greater than or equal to the specified threshold are included in the results.

Proto definition:

.. code-block::

    message MinScoreQuery {
        // The query whose results will be filtered by the minimum score threshold
        Query query = 1;
        // Minimum score threshold; documents with scores below this value are excluded.
        // Must be a non-negative number.
        float min_score = 2;
    }

Example usage:

.. code-block::

    query {
        minScoreQuery {
            query {
                matchQuery {
                    field: "text_field"
                    query: "search terms"
                }
            }
            min_score: 0.5
        }
    }
