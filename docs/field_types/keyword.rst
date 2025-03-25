Keyword
=======
Field to store text data without any tokenization or analysis. Use this when you want to query for exact term values.

.. code-block:: protobuf

    message Field {
        string name = 1;
        FieldType type = 2;
        bool search = 3;
        bool store = 4;
        bool storeDocValues = 5;
        bool multiValued = 9;
        IndexOptions indexOptions = 15;
        bool eagerFieldGlobalOrdinals = 30;
        TextDocValuesType textDocValuesType = 33;
        optional int32 ignoreAbove = 36;
    }

- **name**: Name of the field.
- **type**: Type of the field. Must be set to ATOM.
- **search**: Whether the field should be indexed for search. Default is false.
- **store**: Whether the field should be stored in the index. Default is false.
- **storeDocValues**: Whether the field should be stored in doc values. Default is false.
- **multiValued**: Whether the field can contain more than one value. Default is false.
- **indexOptions**: How the field should be indexed. One of:
    - **DOCS**: Only documents are indexed: term frequencies and positions are omitted. (default)
    - **DOCS_AND_FREQS**: Only documents and term frequencies are indexed: positions are omitted.
    - **DOCS_AND_FREQS_AND_POSITIONS**: Indexes documents, frequencies and positions.
    - **DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS**: Indexes documents, frequencies, positions and offsets.
- **eagerFieldGlobalOrdinals**: Whether to eagerly compute global ordinals for the field. Only applies when using sorted doc values for aggregations. Default is false.
- **textDocValuesType**: Type of doc values used for the field. One of:
    - **TEXT_DOC_VALUES_TYPE_SORTED**: Sorted doc values that use ordinals. Good for aggregation and when there are not many unique terms. Each term must have a byte-length of no more than 32766. (default)
    - **TEXT_DOC_VALUES_TYPE_BINARY**: Binary doc values that store the raw bytes of the term. Good for when there are many unique terms or when terms are longer than 32766 bytes. One usable for single value fields.
- **ignoreAbove**: Skip indexing terms that exceed this length. For multi valued fields, ignoreAbove will be applied for each term separately. This option is useful for protecting against Lucene’s term byte-length limit. Default is no limit.

Example
-------
.. code-block:: json

    {
        "name": "keyword_field",
        "type": "ATOM",
        "search": true,
        "storeDocValues": true
    }