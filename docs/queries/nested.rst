Nested Query
==========================

Searches over nested documents using ``ToParentBlockJoinQuery`` in Lucene. Finds child documents matching the inner query and joins them back to their parent document.

Requires the target field to be registered with ``nestedDoc: true``. See :doc:`/field_types/object`.

Proto definition:

.. code-block:: protobuf

    message NestedQuery {
        enum ScoreMode {
            NONE = 0;
            AVG = 1;
            MAX = 2;
            MIN = 3;
            SUM = 4;
        }
        Query query = 1;          // query applied to child documents
        string path = 2;          // _nested_path of the child documents to search
        ScoreMode scoreMode = 3;  // how child document scores roll up to the parent score
    }

- **query**: Applied to the child documents at ``path``. Only child documents matching this query contribute to the parent result.
- **path**: The dot-separated field name of the nested OBJECT field (e.g. ``"orders"`` or ``"orders.items"``). Must be registered with ``nestedDoc: true``.
- **scoreMode**: How the scores of matching child documents are combined into the parent document's score. ``NONE`` (default) does not propagate child scores.

Single-Level Example
--------------------

Find root documents that have at least one ``orders`` child with ``price < 10``:

.. code-block:: json

    {
        "nestedQuery": {
            "path": "orders",
            "scoreMode": "MAX",
            "query": {
                "rangeQuery": {
                    "field": "orders.price",
                    "upper": "10"
                }
            }
        }
    }

Multi-Level Nested Query
------------------------

For schemas with nested objects inside nested objects (e.g. ``orders`` → ``items``), wrap ``NestedQuery`` calls to join each level in turn. The inner query joins item docs to their order parents; the outer query joins matching order docs to root.

Find root documents that have an order containing an item named "widget":

.. code-block:: json

    {
        "nestedQuery": {
            "path": "orders",
            "query": {
                "nestedQuery": {
                    "path": "orders.items",
                    "query": {
                        "termQuery": {
                            "field": "orders.items.item_name",
                            "textValue": "widget"
                        }
                    }
                }
            }
        }
    }

The ``path`` of each ``NestedQuery`` must be the **immediate** nested level being searched. The parent filter is automatically derived: for ``path="orders.items"`` the parent filter is ``orders``; for ``path="orders"`` the parent filter is ``_root``.

Combining nested and non-nested conditions
------------------------------------------

Find root documents that have an order named "urgent" containing any item with ``quantity > 5``:

.. code-block:: json

    {
        "nestedQuery": {
            "path": "orders",
            "query": {
                "booleanQuery": {
                    "clauses": [
                        {
                            "occur": "MUST",
                            "query": {
                                "termQuery": {
                                    "field": "orders.order_name",
                                    "textValue": "urgent"
                                }
                            }
                        },
                        {
                            "occur": "MUST",
                            "query": {
                                "nestedQuery": {
                                    "path": "orders.items",
                                    "query": {
                                        "rangeQuery": {
                                            "field": "orders.items.quantity",
                                            "lower": "5"
                                        }
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }
    }

The outer boolean ensures both conditions are satisfied by the **same** order document.

Score Modes
-----------

+----------+----------------------------------------------------+
| NONE     | Child scores are not propagated (default).         |
+----------+----------------------------------------------------+
| AVG      | Average of all matching child scores.              |
+----------+----------------------------------------------------+
| MAX      | Maximum matching child score.                      |
+----------+----------------------------------------------------+
| MIN      | Minimum matching child score.                      |
+----------+----------------------------------------------------+
| SUM      | Sum of all matching child scores.                  |
+----------+----------------------------------------------------+
