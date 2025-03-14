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

Example
-------
.. code-block:: json

    {
        "name": "polygon_field",
        "type": "POLYGON",
        "storeDocValues": true
    }