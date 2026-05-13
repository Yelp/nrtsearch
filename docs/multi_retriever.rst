Multi-Retriever Search
======================

Multi-retriever search allows a single ``SearchRequest`` to run multiple independent retrievers (text and/or KNN) and blend their results into a single ranked list. This is the recommended approach for hybrid search (text + vector) in Nrtsearch.

Overview
--------

A ``MultiRetrieverRequest`` is specified in the ``multiRetriever`` field of ``SearchRequest``. It consists of:

- One or more ``Retriever`` definitions, each running an independent query or KNN search.
- A ``Blender`` that merges the per-retriever hit lists into a single ranked result.

.. note::

   When ``multiRetriever`` is set, the top-level ``query``, ``queryText``, and ``knn`` fields must not be used. Facets, ``querySort``, and additional collectors are also not supported.

Request Structure
-----------------

::

    {
      "indexName": "my_index",
      "topHits": 10,
      "retrieveFields": ["id", "title"],
      "multiRetriever": {
        "retrievers": [ ... ],
        "blender": { ... }
      }
    }

Retrievers
----------

Each ``Retriever`` specifies one leg of the search:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Field
     - Description
   * - ``name``
     - Identifier for this retriever (used in diagnostics and retrieverScores).
   * - ``textRetriever``
     - Runs a standard text query. Specify a ``query`` and ``topHits``.
   * - ``knnRetriever``
     - Runs a KNN vector search. Specify a ``knnQuery``.
   * - ``boost``
     - Optional float multiplier applied to scores before blending.
   * - ``rescorer``
     - Optional per-retriever L1 rescorer applied before blending.

Text Retriever
^^^^^^^^^^^^^^

::

    {
      "name": "text",
      "textRetriever": {
        "query": {
          "matchQuery": { "field": "body", "query": "coffee shop" }
        },
        "topHits": 50
      }
    }

KNN Retriever
^^^^^^^^^^^^^

::

    {
      "name": "knn",
      "knnRetriever": {
        "knnQuery": {
          "field": "embedding",
          "queryVector": [0.1, 0.2, 0.3],
          "numCandidates": 100,
          "k": 50
        }
      }
    }

Blenders
--------

The ``blender`` field controls how per-retriever results are merged.

Weighted RRF (Reciprocal Rank Fusion)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Combines retrievers using Reciprocal Rank Fusion. Each document's score is::

    score = boost * (1 / (rankConstant + rank))

summed across all retrievers it appears in. Documents present in multiple retrievers receive a higher combined score.

::

    "blender": {
      "weightedRrf": {
        "rankConstant": 60
      }
    }

``rankConstant`` (default: 60) dampens the impact of rank differences. A lower value makes the top ranks matter more.

Weighted Score Order
^^^^^^^^^^^^^^^^^^^^^

Combines per-retriever scores directly. The ``scoreMode`` determines how multiple scores are merged.

::

    "blender": {
      "weightedScoreOrder": {
        "scoreMode": "SUM"
      }
    }

``scoreMode`` options: ``MAX``, ``SUM``, ``AVG``.

Scoreless Raw Merge
^^^^^^^^^^^^^^^^^^^

Merges all retriever results without applying scores. Useful when you want to union document sets and rank them downstream (e.g., via a rescorer).

::

    "blender": {
      "scorelessRawMerge": {}
    }

Plugin Blender
^^^^^^^^^^^^^^

A custom blending strategy provided by a plugin::

    "blender": {
      "plugin": {
        "name": "my_custom_blender",
        "params": { ... }
      }
    }

Diagnostics
-----------

Multi-retriever diagnostics are reported in ``SearchResponse.diagnostics.multiRetrieverDiagnostics``:

- ``retrieverDiagnostics``: map from retriever name to per-retriever stats (search time, rescore time, total hits, vector diagnostics).
- ``blenderTimeMs``: time spent merging results.

Full Example
------------

Hybrid search combining a text query and KNN vector search, blended with WeightedRRF::

    {
      "indexName": "my_index",
      "startHit": 0,
      "topHits": 10,
      "retrieveFields": ["id", "title"],
      "multiRetriever": {
        "retrievers": [
          {
            "name": "text",
            "textRetriever": {
              "query": {
                "matchQuery": { "field": "text_field", "query": "coffee shop" }
              },
              "topHits": 50
            }
          },
          {
            "name": "knn",
            "knnRetriever": {
              "knnQuery": {
                "field": "vector_field",
                "queryVector": [1.0, 0.0, 0.0],
                "numCandidates": 100,
                "k": 50
              }
            }
          }
        ],
        "blender": {
          "weightedRrf": { "rankConstant": 60 }
        }
      }
    }

See Also
--------

- :doc:`vector_search` — configuring KNN vector fields and standalone KNN queries.
- :doc:`script_rescorer` — re-ranking blended results with a custom score expression.
