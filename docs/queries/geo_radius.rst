Geo-Radius Query
==========================

A query that matches documents with geo point within the radius of provided geo point.

Proto definition:

.. code-block::

   message GeoRadiusQuery {
       string field = 1; // Field in the document to query
       google.type.LatLng center = 2; // target center geo point to calculate distance
       string radius = 3; // distance radius  like "12 km". supports m, km and mi, default to m
   }