Geo-Bounding Box Query
==========================

A query that matches documents with a geopoint within the geo box defined by topLeft and bottomRight latitude-longitude coordinates.

Proto definition:

.. code-block::

   message GeoBoundingBoxQuery {
       string field = 1; // Field in the document to query
       google.type.LatLng topLeft = 2; // top left corner of the geo box
       google.type.LatLng bottomRight = 3; // bottom right corner of the geo box
   }