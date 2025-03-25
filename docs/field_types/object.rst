Object
=======
Field used to store json object data.

.. code-block:: protobuf

    message Field {
        string name = 1;
        FieldType type = 2;
        bool store = 4;
        bool storeDocValues = 5;
        repeated Field childFields = 26;
        bool nestedDoc = 28;
    }

- **name**: Name of the field.
- **type**: Type of the field. Must be set to OBJECT.
- **store**: Whether the field should be stored in the index. Only applies to non-nested objects. Default is false.
- **storeDocValues**: Whether the field should be stored in doc values. Only applies to non-nested objects. Default is false.
- **childFields**: List of definitions for object fields that should be indexed.
- **nestedDoc**: Whether the objects should be indexed as nested documents. Default is false.

Example
-------
.. code-block:: json

    {
        "name": "object_field",
        "type": "OBJECT",
        "storeDocValues": true,
        "childFields": [
            {
                "name": "field1",
                "type": "TEXT",
                "search": true,
            },
            {
                "name": "field2",
                "type": "INT",
                "search": true,
            }
        ]
    }

Objects that contains two fields, one of type TEXT and the other of type INT. The object is stored for retrieval from
doc values. The child fields are indexed for search, usable with the names object_field.field1 and object_field.field2.