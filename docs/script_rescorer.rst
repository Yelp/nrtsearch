Script Rescorer
===============

The ``ScriptRescorer`` re-scores a candidate set of documents using a script expression evaluated per document. It is applied as a ``Rescorer`` in the top-level ``rescorers`` array of a ``SearchRequest``, and works with any query type — standard text queries, KNN searches, or multi-retriever blends.

Overview
--------

After the main retrieval pass, Nrtsearch passes the top ``windowSize`` documents through each rescorer in order. The script is evaluated per document and its return value replaces the document's score.

The scripting language is determined by the ``lang`` field. Nrtsearch ships with built-in support for ``"js"`` (Lucene expressions). Additional languages can be registered by plugins that implement the ``ScriptPlugin`` interface — see `Plugin Languages`_ below.

Request Structure
-----------------

::

    {
      "rescorers": [
        {
          "name": "my_rescorer",
          "windowSize": 100,
          "scriptRescorer": {
            "script": {
              "lang": "js",
              "source": "<expression>"
            }
          }
        }
      ]
    }

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Field
     - Description
   * - ``name``
     - Unique name for this rescorer (required when multiple rescorers are used).
   * - ``windowSize``
     - Number of top documents from the previous pass to rescore.
   * - ``script.lang``
     - Scripting language. Built-in: ``"js"`` (Lucene expressions). Additional languages via plugins.
   * - ``script.source``
     - The script expression to evaluate. Must return a ``double``.
   * - ``script.params``
     - Optional named parameters accessible by name in the expression.

Script Variables
----------------

``_score``
^^^^^^^^^^

The document's score from the prior retrieval pass. For standard queries this is the Lucene relevance score; for multi-retriever requests it is the blended score.

Example: multiply the prior score by 1000::

    "_score * 1000.0"

Document Fields
^^^^^^^^^^^^^^^

Numeric doc-values fields can be referenced directly by name in the expression. Example using a ``popularity`` field::

    "_score * log(1 + popularity)"

Script Parameters
^^^^^^^^^^^^^^^^^

Named parameters can be passed via ``script.params`` and referenced by name in the expression::

    {
      "script": {
        "lang": "js",
        "source": "_score * weight",
        "params": {
          "weight": { "doubleValue": 2.5 }
        }
      }
    }

Built-in Language: ``js`` (Lucene Expressions)
-----------------------------------------------

When ``lang`` is ``"js"``, the source is compiled as a `Lucene expression <https://lucene.apache.org/core/9_0_0/expressions/index.html>`_. Supported syntax:

- Arithmetic operators: ``+``, ``-``, ``*``, ``/``
- Built-in math functions: ``min``, ``max``, ``abs``, ``log``, ``sqrt``, ``pow``, ``ln``, ``exp``, etc.
- Document numeric doc-values fields by name
- The ``_score`` variable

Note that this is not full JavaScript — only the expression subset above is available, and text fields cannot be accessed directly.

Plugin Languages
----------------

Additional scripting languages can be registered by implementing ``ScriptPlugin`` in a server plugin. The plugin provides one or more ``ScriptEngine`` implementations, each identified by a unique ``lang`` string. Once registered, that ``lang`` value can be used in any ``Script`` message throughout Nrtsearch, including in the ``ScriptRescorer``.

Examples
--------

Standard Query with Rescorer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Re-rank text search results using a numeric field::

    {
      "indexName": "my_index",
      "topHits": 10,
      "query": {
        "matchQuery": { "field": "body", "query": "coffee shop" }
      },
      "rescorers": [
        {
          "name": "popularity_boost",
          "windowSize": 100,
          "scriptRescorer": {
            "script": {
              "lang": "js",
              "source": "_score + ln(1 + review_count)"
            }
          }
        }
      ]
    }

Multi-Retriever with Rescorer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Scale scores after a multi-retriever blend::

    {
      "indexName": "my_index",
      "topHits": 10,
      "multiRetriever": {
        "retrievers": [
          {
            "name": "text",
            "textRetriever": {
              "query": { "matchQuery": { "field": "body", "query": "coffee shop" } },
              "topHits": 50
            }
          },
          {
            "name": "knn",
            "knnRetriever": {
              "knnQuery": {
                "field": "embedding",
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
      },
      "rescorers": [
        {
          "name": "scale_score",
          "windowSize": 10,
          "scriptRescorer": {
            "script": { "lang": "js", "source": "_score * 1000.0" }
          }
        }
      ]
    }

Chaining Rescorers
------------------

Multiple rescorers can be chained. Each rescorer's output score becomes ``_score`` for the next::

    "rescorers": [
      {
        "name": "first_pass",
        "windowSize": 100,
        "scriptRescorer": { "script": { "lang": "js", "source": "_score * popularity" } }
      },
      {
        "name": "second_pass",
        "windowSize": 20,
        "scriptRescorer": { "script": { "lang": "js", "source": "_score + recency_boost" } }
      }
    ]

Per-Retriever Rescoring (L1)
-----------------------------

A ``Rescorer`` can also be placed on an individual retriever inside a ``MultiRetrieverRequest``, applied before blending. This is referred to as L1 rescoring::

    {
      "name": "text",
      "textRetriever": {
        "query": { "matchQuery": { "field": "body", "query": "coffee shop" } },
        "topHits": 50
      },
      "rescorer": {
        "name": "l1_boost",
        "windowSize": 50,
        "scriptRescorer": {
          "script": { "lang": "js", "source": "_score * 2.0" }
        }
      }
    }

See Also
--------

- :doc:`multi_retriever` — running multiple retrievers and blending their results.
- :doc:`vector_search` — configuring KNN vector fields.
