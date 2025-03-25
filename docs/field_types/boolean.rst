Boolean
=======
The boolean field type is used to store boolean values.

.. code-block:: protobuf

    message Field {
        string name = 1;
        FieldType type = 2;
        bool search = 3;
        bool store = 4;
        bool storeDocValues = 5;
        bool multiValued = 9;
    }

- **name**: Name of the field.
- **type**: Type of the field. Must be set to BOOLEAN.
- **search**: Whether the field should be indexed for search. Default is false.
- **store**: Whether the field should be stored in the index. Default is false.
- **storeDocValues**: Whether the field should be stored in doc values. Default is false.
- **multiValued**: Whether the field can contain more than one value. Default is false.

Example
-------
.. code-block:: json

    {
        "name": "bool_field",
        "type": "BOOLEAN",
        "search": true,
        "storeDocValues": true
    }