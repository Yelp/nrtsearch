Constant Score Query
==========================

A query that produces a score of 1.0 (modifiable by query boost value) for documents that match the filter query.

Proto definition:

.. code-block::

    message ConstantScoreQuery {
        // Query to determine matching documents
        Query filter = 1;
    }