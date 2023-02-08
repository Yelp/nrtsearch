Highlighting
==========================

Highlights are used to retrieve the information about matched terms in a hit. Nrtsearch currently supports the Fast Vector Highlighter in Lucene.

Custom highlighters can be provided with HighlighterPlugin interface.

Requirements
------------

To be able to highlight a field the following settings must be enabled when registering the field:

general:
^^^^^^^^
* The field must be a TEXT field
* The field must be searchable, i.e. have "search: true"

fast-vector-highlighter:
^^^^^^^^^^^^^^^^^^^^^^^^

* The field must be stored, i.e. have "store: true"
* The field must have term vectors with positions and offsets, i.e. have "termVectors: TERMS_POSITIONS_OFFSETS"
* While not mandatory, the field must be tokenized for the highlights to be useful, i.e. have "tokenize: true" (We recently loose this restriction to allow highlighting on the non-tokenized fields.)

Query Syntax
------------

This is the proto definition for Highlight message which can be specified in SearchRequest:

.. code-block:: protobuf

  // Specify how to highlight matched text in SearchRequest
  message Highlight {

    enum Type {
        // The default highlighter in global settings is FAST_VECTOR; In field_settings is no-override (use global settings)
        DEFAULT = 0;
        FAST_VECTOR = 1;
        PLAIN = 2;  // not supported yet
        CUSTOM = 3;
    }

    message Settings {
        // Specify type of highlighter to use. Ignored right now in nrtsearch.
        Type highlighter_type = 1;
        // Used along with post_tags to specify how to wrap the highlighted text
        repeated string pre_tags = 2;
        // Used along with pre_tags to specify how to wrap the highlighted text
        repeated string post_tags = 3;
        // Number of characters in highlighted fragment, 100 by default
        google.protobuf.UInt32Value fragment_size = 4;
        // Maximum number of highlight fragments to return, 5 by default. If set to 0 returns entire text as a single fragment ignoring fragment_size.
        google.protobuf.UInt32Value max_number_of_fragments = 5;
        // Specify a query here if highlighting is desired against a different query than the search query
        Query highlight_query = 6;
        // Set to true to highlight fields only if specified in the search query.
        // This field is identical to field_match_v2 but doesn't support field override. This setting will be ignored if field_match_v2 is present.
        bool field_match = 7 [deprecated = true];
        // Set to true to highlight fields only if specified in the search query. (Default is false)
        google.protobuf.BoolValue field_match_v2 = 8;
        // Sorts highlighted fragments by score when set to true. By default, fragments will be output in the order they appear in the field. (Default is true)
        google.protobuf.BoolValue score_ordered = 9;
        // Select Fragmenter between span (default) and simple. This is only applicable for plain highlighters.
        google.protobuf.StringValue fragmenter = 10;
        // Let the fragment builder respect the multivalue fields. Each fragment won't cross multiple value fields if set true. (Default is false)
        google.protobuf.BoolValue discrete_multivalue = 11;
        // When highlighter_type is CUSTOM, use this string identifier to specify the highlighter. It is ignored for any other highlighter_types.
        string custom_highlighter_name = 12;
    }

    // Highlight settings
    Settings settings = 1;
    // Fields to highlight
    repeated string fields = 2;
    // Map of field name to highlight settings for field, overrides request level highlight settings
    map<string, Settings> field_settings = 3;
  }

* Since Nrtsearch only uses *fast-vector-highlighter* right now, the *default* highlighter is set to be *fast-vector-highlighter*. If you intend to always use *fast-vector-highlighter* even if the *default* highlighter changes, you should set ``highlighter_type`` to ``FAST_VECTOR``.
* The fields to highlight must be provided in "fields". The Settings can be provided for all fields or per-field. You can also provide global settings then override individual settings for each field to be highlighted.
* The field ``field_match`` (v1) is deprecated as it doesn't support the field override. For any new client, please use ``field_match_v2`` instead. ``field_match`` will only take effect when ``field_match_v2`` is omitted. And ``field_match`` (v1) will be removed eventually.

Example Queries
---------------

Below examples use a simple index called "test_index" with two fields doc_id and comment. The index has two documents:

.. table:: Documents in test_index
   :widths:

======= =====================================================================================================================================================
doc_id  comment
======= =====================================================================================================================================================
1       the food here is amazing, service was good
2       This is my first time eating at this restaurant. The food here is pretty good, the service could be better. My favorite food was chilly chicken.
======= =====================================================================================================================================================
Simple query with default settings for highlights:

.. code-block:: json

  {
    "indexName": "test_index",
    "topHits": 2,
    "query": {
      "matchQuery": {
        "field": "comment",
        "query": "food"
      }
    },
    "highlight": {
      "fields": ["comment"]
    }
  }

Highlights in the response for above request:

.. code-block:: json

  {
    "hits": [{
      "highlights": {
        "comment": {
          "fragments": ["the <em>food</em> here is amazing, service was good"]
        }
      }
    }, {
      "highlights": {
        "comment": {
          "fragments": ["restaurant. The <em>food</em> here is pretty good, the service could be better. My favorite <em>food</em> was chilly chicken"]
        }
      }
    }]
  }

Example search request which more custom options for highlighting:

.. code-block:: json

  {
    "indexName": "test_index",
    "topHits": 2,
    "query": {
      "matchQuery": {
        "field": "comment",
        "query": "food"
      }
    },
    "highlight": {
      "fields": ["comment"],
      "fieldSettings": {
        "comment": {
          "preTags": ["<START>"],
          "postTags": ["<END>"],
          "fragmentSize": 18,
          "maxNumberOfFragments": 3,
          "highlightQuery": {
            "matchQuery": {
              "field": "comment",
              "query": "food is good"
            }
          }
        }
      }
    }
  }

Highlights in the response for above request:

.. code-block:: json

  {
    "hits": [{
      "highlights": {
        "comment": {
          "fragments": ["the <START>food<END> here <START>is<END> amazing", "service was <START>good<END>"]
        }
      }
    }, {
      "highlights": {
        "comment": {
          "fragments": ["The <START>food<END> here <START>is<END> pretty", "This <START>is<END> my first time", "pretty <START>good<END>, the service"]
        }
      }
    }]
  }
