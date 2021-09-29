Term Query
==========================

A query that matches documents containing a term.

Proto definition:

.. code-block::

   message TermQuery {
       // Field in the document to query.
       string field = 1;

       oneof TermTypes {
           // TEXT FieldType term to search for.
           string textValue = 2;
           // INT FieldType term to search for.
           int32 intValue = 3;
           // LONG FieldType term to search for.
           int64 longValue = 4;
           // FLOAT FieldType term to search for.
           float floatValue = 5;
           // DOUBLE FieldType term to search for.
           double doubleValue = 6;
           // BOOLEAN FieldType term to search for.
           bool booleanValue = 7;
       }
   }