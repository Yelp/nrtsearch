Geo-Polygon Query
==========================

A query that matches documents with geo point within a defined set of polygons.

Polygon definitions must conform to the `GeoJson <https://geojson.org/>`_ standard.
Polygons must not be self-crossing, otherwise may result in unexpected behavior.
Polygons cannot cross the 180th meridian. Instead, use two polygons: one on each side.

Proto definition:

.. code-block::

   message GeoPolygonQuery {
       // Field in the document to query
       string field = 1;
       // Geo polygons to search for containing points
       repeated Polygon polygons = 2;
   }