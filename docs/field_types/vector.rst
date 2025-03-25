Vector
==========================
Field for storing dense vectors with a fixed number of dimensions.

.. code-block:: protobuf

    message Field {
        string name = 1;
        FieldType type = 2;
        bool search = 3;
        bool storeDocValues = 5;
        int32 vectorDimensions = 29;
        string vectorSimilarity = 31;
        VectorIndexingOptions vectorIndexingOptions = 32;
        VectorElementType vectorElementType = 34;
    }

- **name**: Name of the field.
- **type**: Type of the field. Must be set to VECTOR.
- **search**: Whether the field should be indexed for vector search. Default is false.
- **storeDocValues**: Whether the field should be stored in doc values. If search is true, this is not needed since the data can be retrieved from the index. Default is false.
- **vectorDimensions**: Number of dimensions in the vector. Must be <= 4096.
- **vectorElementType**: Type of the elements in the vector. Must be one of:
    - **VECTOR_ELEMENT_FLOAT**: Single precision floating point (default)
    - **VECTOR_ELEMENT_BYTE**: Signed byte
- **vectorSimilarity**: Similarity function to use for vector search. Must be one of:
    - **l2_norm**: (1 / (1 + l2_norm(query, vector)^2))
    - **dot_product**:
        - Float vector: ((1 + dot_product(query, vector)) / 2) (all vectors must be unit length)
        - Byte vector : 0.5 + (dot_product(query, vector) / (32768 * dims)) (all vectors must have the same length)
    - **cosine**: ((1 + cosine(query, vector)) / 2)
    - **normalized_cosine**: Only available for float vectors. Identical usage to 'cosine', but indexed and query vectors are automatically normalized to unit length to allow for use of faster dot product for comparisons. Original vector magnitude is available in the <field>._magnitude field.
    - **max_inner_product**:
        - when < 0 : 1 / (1 + -1 * max_inner_product(query, vector))
        - when >= 0: max_inner_product(query, vector) + 1
- **vectorIndexingOptions**: Options for indexing the vector when search is true. See section below for details.

Vector Indexing Options
-----------------------
.. code-block:: protobuf

    message VectorIndexingOptions {
        optional string type = 1;
        optional int32 hnsw_m = 2;
        optional int32 hnsw_ef_construction = 3;
        optional int32 merge_workers = 4;
        optional float quantized_confidence_interval = 5;
        optional int32 quantized_bits = 6;
        optional bool quantized_compress = 7;
    }

- **type**: Type of indexing to use. Must be one of:
    - **hnsw**: Hierarchical Navigable Small World graph based vector search. (default)
    - **hnsw_scalar_quantized**: Only available for float vectors. Uses scalar quantization to reduce the number of bits needed to store the vectors. Allows for trade-off between accuracy and memory usage.
- **hnsw_m**: Number of of neighbors each node will be connected to in the HNSW graph. Default is 16.
- **hnsw_ef_construction**: Number of candidates to evaluate during construction of the HNSW graph. Default is 100.
- **merge_workers**: Number of threads to use for merging the HNSW graph during segment merges. Default is 1.
- **quantized_confidence_interval**: Confidence interval for quantization. When unset, it is calculated based on the vector dimension. When 0, the quantiles are dynamically determined by sampling many confidence intervals and determining the most accurate pair. Otherwise, the value must be between 0.9 and 1.0 (both inclusive). Default is unset.
- **quantized_bits**: Number of bits to use for quantization. Must be one of:
    - 4 - half byte
    - 7 - signed byte (default)
- **quantized_compress**: Whether to compress the quantized vectors. If true, the vectors that are quantized with <= 4 bits will be compressed into a single byte. If false, the vectors will be stored as is. This provides a trade-off of memory usage and speed. Default is false.

Example Field
-------------
.. code-block:: json

    {
        "name": "vector_field",
        "type": "VECTOR",
        "search": true,
        "storeDocValues": false,
        "vectorDimensions": 128,
        "vectorElementType": "VECTOR_ELEMENT_FLOAT",
        "vectorSimilarity": "l2_norm",
        "vectorIndexingOptions": {
            "type": "hnsw",
            "hnsw_m": 16,
            "hnsw_ef_construction": 100,
        }
    }

This field will store 128-dimensional float vectors using the L2 norm similarity function and HNSW indexing.

Ingestion Data Format
---------------------
Single string encoding the vector data as a json array. The array must have the same number of elements as the vectorDimensions specified in the field definition.

Example AddDocumentRequest:

.. code-block:: json

    {
        "indexName": "example_index",
        "fields": {
            "vector_field": {
                "value": [
                    "[0.188423157, 0.246743672, 0.14576434]"
                ]
            }
        }
    }