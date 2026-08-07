Geo-Polygon Query
==========================

A query that matches documents based on a defined set of query polygons. The behavior depends on the field type:

- **LAT_LON**: matches documents whose indexed *point* falls within the query polygon(s).
- **POLYGON**: matches documents whose indexed *polygon* intersects the query polygon(s).

Query polygon definitions must conform to the `GeoJson <https://geojson.org/>`_ standard.
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

Supported field types: ``LAT_LON``, ``POLYGON``