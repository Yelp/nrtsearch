ID Field
========
This field serves as the primary term for the document. All indices are required to have an _ID field, and all documents are required to have a value for this field. When updating a document, the _ID field is used to atomically replace the previous document with the new version.

.. code-block:: protobuf

    message Field {
        string name = 1;
        FieldType type = 2;
        bool store = 4;
        bool storeDocValues = 5;
    }

- **name**: Name of the field.
- **type**: Type of the field. Must be set to BOOLEAN.
- **store**: Whether the field should be stored in the index. Default is false.
- **storeDocValues**: Whether the field should be stored in doc values. Default is false.

An _ID field is always searchable.

Either store or storeDocValues must be set to true.

Example
-------
.. code-block:: json

    {
        "name": "id_field",
        "type": "_ID",
        "storeDocValues": true
    }