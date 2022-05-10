Term-In-Set Query
==========================

Matches documents that contains at least one of the provided terms. Only the type of terms that matches the field type must be provided. This maps to Lucene's TermInSet query.

Proto definition:

.. code-block::

   message TermInSetQuery {
       // Field in the document to query.
       string field = 1;

       message TextTerms {
           repeated string terms = 1;
       }
       message IntTerms {
           repeated int32 terms = 1;
       }
       message LongTerms {
           repeated int64 terms = 1;
       }
       message FloatTerms {
           repeated float terms = 1;
       }
       message DoubleTerms {
           repeated double terms = 1;
       }

       oneof TermTypes {
           // Text terms to search for.
           TextTerms textTerms = 2;
           // Int terms to search for.
           IntTerms intTerms = 3;
           // Long terms to search for.
           LongTerms longTerms = 4;
           // Float terms to search for.
           FloatTerms floatTerms = 5;
           // Double terms to search for.
           DoubleTerms doubleTerms = 6;
       }
   }