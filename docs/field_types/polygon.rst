Polygon
=======
Field used for closed geo polygons represented as a list of geo points. The first and last points must the the same.

.. code-block:: protobuf

    message Field {
        string name = 1;
        FieldType type = 2;
        bool store = 4;
        bool storeDocValues = 5;
    }

- **name**: Name of the field.
- **type**: Type of the field. Must be set to POLYGON.
- **store**: Whether the field should be stored in the index. Default is false.
- **storeDocValues**: Whether the field should be stored in doc values. Default is false.

This field is always searchable.

Supported Queries
-----------------
The POLYGON field type supports the following query types. All geo queries use ``INTERSECTS`` semantics — a document matches if the indexed polygon intersects the query shape.

- :doc:`/queries/geo_point` — find indexed polygons that **contain** the given point
- :doc:`/queries/geo_radius` — find indexed polygons that intersect a circle (center point + radius)
- :doc:`/queries/geo_bounding_box` — find indexed polygons that intersect a bounding box
- :doc:`/queries/geo_polygon` — find indexed polygons that intersect the given query polygon(s)

Example
-------
.. code-block:: json

    {
        "name": "polygon_field",
        "type": "POLYGON",
        "storeDocValues": true
    }