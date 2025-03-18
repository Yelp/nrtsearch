Text
=======
Field for full text search. Text values are tokenized and analyzed for search.

.. code-block:: protobuf

    message Field {
        string name = 1;
        FieldType type = 2;
        bool search = 3;
        bool store = 4;
        bool storeDocValues = 5;
        bool multiValued = 9;
        bool omitNorms = 11;
        IndexOptions indexOptions = 15;
        Analyzer analyzer = 17;
        Analyzer indexAnalyzer = 18;
        Analyzer searchAnalyzer = 19;
        TermVectors termVectors = 20;
        string similarity = 21;
        google.protobuf.Struct similarityParams = 25;
        bool eagerFieldGlobalOrdinals = 30;
        TextDocValuesType textDocValuesType = 33;
        optional int32 positionIncrementGap = 35;
        optional int32 ignoreAbove = 36;
    }

- **name**: Name of the field.
- **type**: Type of the field. Must be set to ATOM.
- **search**: Whether the field should be indexed for search. Default is false.
- **store**: Whether the field should be stored in the index. Default is false.
- **storeDocValues**: Whether the field should be stored in doc values. Default is false.
- **multiValued**: Whether the field can contain more than one value. Default is false.
- **omitNorms**: Whether norms should be omitted for the field. Disables length normalization boosting, where shorter field values get a higher score. Default is false.
- **indexOptions**: How the field should be indexed. One of:
    - **DOCS**: Only documents are indexed: term frequencies and positions are omitted.
    - **DOCS_AND_FREQS**: Only documents and term frequencies are indexed: positions are omitted.
    - **DOCS_AND_FREQS_AND_POSITIONS**: Indexes documents, frequencies and positions. (default)
    - **DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS**: Indexes documents, frequencies, positions and offsets.
- **analyzer**: Analyzer used for indexing and searching the field. One of:
    - **standard**: Standard analyzer. (default)
    - **classic**: Classic analyzer.
    - Loaded by class name template "org.apache.lucene.analysis.{analyzer}Analyzer".
    - Custom analyzer registered by a plugin.
- **indexAnalyzer**: Analyzer used for indexing the field. Default is the analyzer.
- **searchAnalyzer**: Analyzer used for searching the field. Default is the analyzer.
- **termVectors**: Whether term vectors should be stored for the field. One of:
    - **NO_TERMVECTORS**: No term vectors are stored. (default)
    - **TERMS**: Index terms only.
    - **TERMS_POSITIONS**: Index terms and positions.
    - **TERMS_POSITIONS_OFFSETS**: Index terms, positions and offsets.
    - **TERMS_POSITIONS_OFFSETS_PAYLOADS**: Index terms, positions, offsets and payloads.
- **similarity**: Similarity used for scoring the field. One of:
    - **BM25**: BM25 similarity. (default)
    - **classic**: Classic similarity.
    - **boolean**: Boolean similarity.
    - Custom similarity registered by a plugin.
- **similarityParams**: Parameters for similarity implementation.
- **eagerFieldGlobalOrdinals**: Whether to eagerly compute global ordinals for the field. Only applies when using sorted doc values for aggregations. Default is false.
- **textDocValuesType**: Type of doc values used for the field. One of:
    - **TEXT_DOC_VALUES_TYPE_SORTED**: Sorted doc values that use ordinals. Good for aggregation and when there are not many unique terms. Each term must have a byte-length of no more than 32766. (default)
    - **TEXT_DOC_VALUES_TYPE_BINARY**: Binary doc values that store the raw bytes of the term. Good for when there are many unique terms or when terms are longer than 32766 bytes. One usable for single value fields.
- **positionIncrementGap**: The position increment gap between multi valued fields. Default is 100.
- **ignoreAbove**: Skip indexing terms that exceed this length. For multi valued fields, ignoreAbove will be applied for each term separately. This option is useful for protecting against Lucene’s term byte-length limit. Default is no limit.

Example
-------
.. code-block:: json

    {
        "name": "text_field",
        "type": "TEXT",
        "search": true,
        "storeDocValues": true,
        "analyzer": {
            "name": "standard"
        }
    }