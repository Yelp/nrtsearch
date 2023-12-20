Geo-Polygon Query
==========================

A query that matches documents with geo point within a defined set of polygons.

Proto definition:

.. code-block::

   message GeoPolygonQuery {
       // Field in the document to query
       string field = 1;
       // Geo polygons to search for containing points
       repeated Polygon polygons = 2;
   }