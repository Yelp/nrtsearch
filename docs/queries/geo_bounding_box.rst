Geo-Bounding Box Query
==========================

A query that matches documents within the geo box defined by topLeft and bottomRight latitude-longitude coordinates. The behavior depends on the field type:

- **LAT_LON**: matches documents whose indexed *point* falls within the box.
- **POLYGON**: matches documents whose indexed *polygon* intersects the box.

Proto definition:

.. code-block::

   message GeoBoundingBoxQuery {
       string field = 1; // Field in the document to query
       google.type.LatLng topLeft = 2; // top left corner of the geo box
       google.type.LatLng bottomRight = 3; // bottom right corner of the geo box
   }

Supported field types: ``LAT_LON``, ``POLYGON``