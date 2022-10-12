Highlighting
==========================

Highlights are used to retrieve the information about matched terms in a hit. Nrtsearch currently supports the Fast Vector Highlighter in Lucene.

Requirements
------------

To be able to highlight a field the following settings must be enabled when registering the field:

* The field must be a TEXT field
* The field must be searchable, i.e. have "search: true"
* The field must be stored, i.e. have "store: true"
* The field must have term vectors with positions and offsets, i.e. have "termVectors: TERMS_POSITIONS_OFFSETS"
* While not mandatory, the field must be tokenized for the highlights to be useful, i.e. have "tokenize: true"

Query Syntax
------------

This is the proto definition for Highlight message which can be specified in SearchRequest:

.. code-block::

 // Specify how to highlight matched text in SearchRequest
 message Highlight {

     enum Type {
         DEFAULT = 0;
         FAST_VECTOR = 1;
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
         // Set to true to highlight fields only if specified in the search query
         bool field_match = 7;
     }

     // Highlight settings
     Settings settings = 1;
     // Fields to highlight
     repeated string fields = 2;
     // Map of field name to highlight settings for field, overrides request level highlight settings
     map<string, Settings> field_settings = 3;
 }

Since Nrtsearch only uses fast vector highlighter right now highlighter_type is ignored. If you intend to always use fast vector highlighter even if the default highlighter changes you should set this parameter to FAST_VECTOR.
The fields to highlight must be provided in "fields". The Settings can be provided for all fields or per-field. You can also provide global settings then override individual settings for each field to be highlighted.

Example Queries
---------------

Below examples use a simple index called "test_index" with two fields doc_id and comment. The index has two documents:

* doc_id: 1, comment: the food here is amazing, service was good
* doc_id: 2, comment: This is my first time eating at this restaurant. The food here is pretty good, the service could be better. My favorite food was chilly chicken.

Simple query with default settings for highlights:

.. code-block::

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

.. code-block::

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

Example search request which specifies custom options for highlighting:

.. code-block::

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

.. code-block::

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
