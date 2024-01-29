Prefix Query
==========================

A query that matches documents that contain a specific prefix in a provided field.

Proto definition:

.. code-block::

   message PrefixQuery {
       // Document field name.
       string field = 1;
       // Prefix to search for.
       string prefix = 2;
       // Method used to rewrite the query.
       RewriteMethod rewrite = 3;
       // Specifies the size to use for the TOP_TERMS* rewrite methods.
       int32 rewriteTopTermsSize = 4;
   }