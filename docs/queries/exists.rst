Exists Query
==========================

A query that matches documents which contain a value for a field.

Proto definition:

.. code-block::

   message ExistsQuery {
       string field = 1; // Field in the document to query
   }