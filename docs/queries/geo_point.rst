Geo-Point Query
==========================

A query that matches documents with a polygon that contains the provided geo point.

Proto definition:

.. code-block::

   message GeoPointQuery {
       string field = 1; // Field in the document to query
       google.type.LatLng point = 2; // point used to query whether the polygon contains it.
   }