InnerHit
==========================

``NestedQuery`` filters parent documents based on child document matches, but the response contains only parent document fields. ``InnerHit`` adds a second-pass search per parent hit to return the matching child documents alongside their parent.

Requirements
------------

The index must have at least one field registered with ``nestedDoc: true``:

.. code-block:: json

    {
        "name": "menu",
        "type": "OBJECT",
        "nestedDoc": true,
        "multiValued": true,
        "childFields": [
            {"name": "food_name", "type": "ATOM", "search": true, "storeDocValues": true},
            {"name": "price",     "type": "INT",  "search": true, "storeDocValues": true}
        ]
    }

Query Syntax
------------

``InnerHit`` is specified in the ``innerHits`` map on ``SearchRequest``, keyed by a name of your choice:

.. code-block:: protobuf

    message InnerHit {
        // Nested path to search against (must be registered with nestedDoc: true)
        string query_nested_path = 1;
        // Which hit to start from (for pagination); default: 0
        int32 start_hit = 2;
        // How many top child hits to return per parent; default: 3
        int32 top_hits = 3;
        // Optional filter applied to child documents
        Query inner_query = 4;
        // Child fields to retrieve
        repeated string retrieve_fields = 5;
        // Sort child hits by field (default: by relevance)
        QuerySortField query_sort = 6;
        // Highlight matching child documents
        Highlight highlight = 7;
    }

- **query_nested_path**: The dot-separated path of the nested OBJECT field to search (e.g. ``"menu"`` or ``"orders.items"``). Works for both single-level and multi-level nested paths.
- **top_hits**: Maximum number of child hits returned per parent document.
- **inner_query**: If omitted, all child documents for each parent are returned (up to ``top_hits``).

Example Queries
---------------

Assuming documents stored in ``index_alpha``:

.. code-block:: yaml

    - business_name: restaurant_A
      business_address: 10 A street
      menu:
        - food_name: chicken
          price: 5
        - food_name: burger
          price: 8
    - business_name: restaurant_B
      business_address: 6 B avenue
      menu:
        - food_name: coke
          price: 4
        - food_name: cheeseburger
          price: 10

Case 1: Get all parents (no InnerHit)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

    {
        "indexName": "index_alpha",
        "retrieveFields": ["business_name"]
    }

Case 2: Get all children per parent
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

    {
        "indexName": "index_alpha",
        "retrieveFields": ["business_name"],
        "innerHits": {
            "menu_hits": {
                "query_nested_path": "menu",
                "top_hits": 10,
                "retrieve_fields": ["menu.food_name"]
            }
        }
    }

Case 3: Filter parents, return all their children
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

    {
        "indexName": "index_alpha",
        "query": {
            "termQuery": {"field": "business_name", "textValue": "restaurant_A"}
        },
        "retrieveFields": ["business_name"],
        "innerHits": {
            "menu_hits": {
                "query_nested_path": "menu",
                "retrieve_fields": ["menu.food_name"]
            }
        }
    }

Case 4: Filter children within each parent
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

    {
        "indexName": "index_alpha",
        "retrieveFields": ["business_name"],
        "innerHits": {
            "cheap_items": {
                "query_nested_path": "menu",
                "inner_query": {
                    "rangeQuery": {"field": "menu.price", "upper": "6"}
                },
                "retrieve_fields": ["menu.food_name", "menu.price"]
            }
        }
    }

Case 5: Filter both parents and children
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

    {
        "indexName": "index_alpha",
        "query": {
            "termQuery": {"field": "business_name", "textValue": "restaurant_A"}
        },
        "retrieveFields": ["business_name"],
        "innerHits": {
            "cheap_items": {
                "query_nested_path": "menu",
                "inner_query": {
                    "rangeQuery": {"field": "menu.price", "upper": "6"}
                },
                "retrieve_fields": ["menu.food_name"]
            }
        }
    }

Multi-Level Nested Paths
------------------------

``InnerHit`` works with multi-level nested paths. For a schema with ``orders`` (nested) containing ``items`` (nested), use the full dot-separated path to target the inner level:

.. code-block:: json

    {
        "indexName": "my_index",
        "retrieveFields": ["doc_id"],
        "innerHits": {
            "all_items": {
                "query_nested_path": "orders.items",
                "top_hits": 100,
                "retrieve_fields": ["orders.items.item_name", "orders.items.quantity"]
            }
        }
    }

This returns all item-level child documents for each root document hit, across all orders. The parent boundary used is the root document — all items from all orders are pooled together per root hit, filtered by ``_nested_path = orders.items``.

Multiple InnerHits
------------------

Multiple ``InnerHit`` entries can be specified on a single request, each with a different name and ``query_nested_path``:

.. code-block:: json

    {
        "indexName": "my_index",
        "retrieveFields": ["doc_id"],
        "innerHits": {
            "orders": {
                "query_nested_path": "orders",
                "retrieve_fields": ["orders.order_name"]
            },
            "items": {
                "query_nested_path": "orders.items",
                "retrieve_fields": ["orders.items.item_name"]
            }
        }
    }
