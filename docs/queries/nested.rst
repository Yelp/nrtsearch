Nested Query
==========================

Searches over nested documents using ToParentBlockJoinQuery in Lucene.

Proto definition:

.. code-block::

   message NestedQuery {
       enum ScoreMode {
           NONE = 0;
           AVG = 1;
           MAX = 2;
           MIN = 3;
           SUM = 4;
       }
       Query query = 1; // query for the child documents
       string path = 2; // field name of the nested
       ScoreMode scoreMode = 3; // how child documents score affects final score
   }