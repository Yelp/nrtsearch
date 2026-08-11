Geo-Radius Query
==========================

A query that finds documents within a given radius of a center point. The behavior depends on the field type:

- **LAT_LON**: matches documents whose indexed *point* falls within the circle.
- **POLYGON**: matches documents whose indexed *polygon* intersects the circle.

Proto definition:

.. code-block::

   message GeoRadiusQuery {
       string field = 1; // Field in the document to query
       google.type.LatLng center = 2; // target center geo point to calculate distance
       string radius = 3; // distance radius  like "12 km". supports m, km and mi, default to m
   }

Supported field types: ``LAT_LON``, ``POLYGON``