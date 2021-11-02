Boolean Query
==========================

A Query that matches documents matching boolean combinations of other queries, e.g. TermQuery, PhraseQuery or other BooleanQuery. This query maps to BooleanQuery in Lucene.
The query consists of one or more clauses, each clause containing an occurrence which defines how the clause may occur in matching documents and a query for the clause. There are four types of occurrences:

  * ``SHOULD`` - the query may appear in matching documents but if the query doesn't appear in a document which is matched by another clause, this clause will still be used for scoring

  * ``MUST`` - the query must appear in matching documents and will be used for scoring

  * ``FILTER`` - the query must appear in matching documents but will not be used for scoring

  * ``MUST_NOT`` - the query must not appear in matching documents and will not be used for scoring

Proto definition:

.. code-block::

   message BooleanQuery {
       repeated BooleanClause clauses = 1; // Clauses for a boolean query.
       int32 minimumNumberShouldMatch = 2; // Minimum number of optional clauses that must match.
   }

   message BooleanClause {
       // Defines how clauses may occur in matching documents. This will always be SHOULD by default.
       enum Occur {
           SHOULD = 0;
           MUST = 1;
           FILTER = 2;
           MUST_NOT = 3;
       }

       Query query = 1; // The Query for the clause.
       Occur occur = 2; // Specifies how this clause must occur in a matching document. SHOULD by default.
   }
