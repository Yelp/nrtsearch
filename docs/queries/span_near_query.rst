Spean Near Query
==========================

A query that matches spans which are near one another. One can specify slop, the maximum number of intervening unmatched positions, as well as whether matches are required to be in-order.


Proto definition:

.. code-block::

    // Wrapper message for different types of SpanQuery
    message SpanQuery {
      oneof query {
        TermQuery spanTermQuery = 1;
        SpanNearQuery spanNearQuery = 2;
        SpanMultiTermQueryWrapper spanMultiTermQueryWrapper = 3;
      }
    }

    // Cannot be constructed directly. Use SpanQuery message to construct.
    message SpanNearQuery {
        // Clauses for a span near query.
        repeated SpanQuery clauses = 1;
        // Maximum number of positions between matching terms.
        int32 slop = 2;
        // True if the matching terms must be in the same order as the query.
        bool inOrder = 3;
    }
