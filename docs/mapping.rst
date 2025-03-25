Mapping
==========================

Mapping defines how a document and its fields are stored and indexed. In NRTSearch, every index is required to have a mapping for it documents.

Field Types
-----------
.. toctree::
   :maxdepth: 1
   :glob:

   field_types/*

Child Fields
------------
By default, all field types support the 'childFields' property. The child fields are used to index the same input data in different ways.
For example, you may want to index the same text with multiple different analyzers. Using child fields allows you to avoid
duplicating the data in the indexing request. This feature can be used with any field types, as long as the same input data can be
processed by each of the field types.

Object fields do not behave this way. The child fields of an object are used to define the fields within an object. See :doc:`field_types/object`.

.. code-block:: json

    {
        "name": "text_field",
        "type": "TEXT",
        "search": true,
        "childFields": [
            {
                "name": "keyword",
                "type": "ATOM",
                "search": true,
                "storeDocValues": true
            }
        ]
    }

The indexed text is indexed with the standard analyzer, and queryable with the field name 'text_field'. The same text is also indexed
with the keyword analyzer, and queryable with the field name 'text_field.keyword'. The keyword field is also stored in doc values for retrieval.