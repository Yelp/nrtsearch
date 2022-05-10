Range Query
==========================

A query that matches documents with values within the specified range. The lower and upper values though provided as strings will be converted to the type of the field. This works with INT, LONG, FLOAT, DOUBLE and DATE_TIME field types.

Proto definition:

.. code-block::

   message RangeQuery {
       string field = 1; // Field in the document to query
       string lower = 2; // Lower bound, inclusive by default
       string upper = 3; // Upper bound, inclusive by default
       bool lowerExclusive = 4; // Set true to make lower bound exclusive
       bool upperExclusive = 5; // Set true to make upper bound exclusive
   }