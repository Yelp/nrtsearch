Object
=======

Field used to store JSON object data. Objects can be indexed in two modes:

- **Flattened** (default): child field values are indexed directly on the parent document. The object structure is only preserved in stored/doc-values form.
- **Nested** (``nestedDoc: true``): each object value is indexed as a separate Lucene child document, preserving cross-field correlation within each object.

.. code-block:: protobuf

    message Field {
        string name = 1;
        FieldType type = 2;
        bool store = 4;
        bool storeDocValues = 5;
        repeated Field childFields = 26;
        bool nestedDoc = 28;
    }

- **name**: Name of the field.
- **type**: Must be ``OBJECT``.
- **store**: Store the raw object for retrieval. Only applies to non-nested objects. Default false.
- **storeDocValues**: Store the object as binary doc values for retrieval. Only applies to non-nested objects. Default false.
- **childFields**: Child field definitions. Each child is addressable as ``object_field.child_name``.
- **nestedDoc**: Index each object value as a separate nested (child) Lucene document. Default false.

Flattened Object
----------------

When ``nestedDoc`` is false, child field values are written directly onto the parent document. Multi-valued objects are indexed such that all values for a given child field are pooled together — a query on ``orders.item_name = "widget"`` and ``orders.item_name = "gadget"`` will match a document that has those names in *different* order objects. Use nested documents (see below) when cross-field correlation matters.

The object itself can be retrieved as a ``Struct`` via ``store: true`` or ``storeDocValues: true``.

.. code-block:: json

    {
        "name": "address",
        "type": "OBJECT",
        "storeDocValues": true,
        "childFields": [
            {"name": "city",  "type": "ATOM", "search": true, "storeDocValues": true},
            {"name": "state", "type": "ATOM", "search": true, "storeDocValues": true}
        ]
    }

Child fields are queried as ``address.city`` and ``address.state``.

Nested Objects (nestedDoc: true)
---------------------------------

When ``nestedDoc`` is true, each value of the field is indexed as an independent Lucene child document in the same segment block as its parent. This preserves cross-field correlation: a query on ``orders.item_name = "widget"`` *and* ``orders.price < 10`` within the same ``NestedQuery`` will only match if both conditions are satisfied by the **same** order object.

The block layout in the Lucene segment is:

.. code-block:: text

    [child doc 1]  _nested_path = <field_name>
    [child doc 2]  _nested_path = <field_name>
    ...
    [parent doc]   _nested_path = _root

``store`` and ``storeDocValues`` are not supported on nested objects.

.. code-block:: json

    {
        "name": "orders",
        "type": "OBJECT",
        "nestedDoc": true,
        "multiValued": true,
        "childFields": [
            {"name": "order_name", "type": "ATOM",  "search": true, "storeDocValues": true},
            {"name": "price",      "type": "FLOAT",  "search": true, "storeDocValues": true}
        ]
    }

Use ``NestedQuery`` to query nested documents and join matches back to the parent. See :doc:`/queries/nested`.

Multi-Level Nested Objects
---------------------------

A nested OBJECT field can itself contain a nested OBJECT child, creating a two-level (or deeper) hierarchy. Each level that has ``nestedDoc: true`` produces its own set of child documents with a distinct ``_nested_path``.

.. code-block:: json

    {
        "name": "orders",
        "type": "OBJECT",
        "nestedDoc": true,
        "multiValued": true,
        "childFields": [
            {"name": "order_name", "type": "ATOM",  "search": true, "storeDocValues": true},
            {
                "name": "items",
                "type": "OBJECT",
                "nestedDoc": true,
                "multiValued": true,
                "childFields": [
                    {"name": "item_name", "type": "ATOM",  "search": true, "storeDocValues": true},
                    {"name": "quantity",  "type": "INT",   "search": true, "storeDocValues": true}
                ]
            }
        ]
    }

The segment block layout for a document with two orders (order1 containing widget+gadget, order2 containing thingamajig):

.. code-block:: text

    [item: widget]       _nested_path = orders.items
    [item: gadget]       _nested_path = orders.items
    [order: order1]      _nested_path = orders
    [item: thingamajig]  _nested_path = orders.items
    [order: order2]      _nested_path = orders
    [root document]      _nested_path = _root

Each ``_parent_offset`` doc-values field on a child document records the distance to its **immediate** parent: item docs point to their order doc, order docs point to root.

To query across multiple levels, wrap ``NestedQuery`` calls:

.. code-block:: json

    {
        "nestedQuery": {
            "path": "orders",
            "query": {
                "nestedQuery": {
                    "path": "orders.items",
                    "query": {
                        "termQuery": {"field": "orders.items.item_name", "textValue": "widget"}
                    }
                }
            }
        }
    }

The inner ``NestedQuery`` joins matching item docs to their parent order docs. The outer ``NestedQuery`` joins matching order docs to the root document. See :doc:`/queries/nested` for full details.

Searching at a Nested Level (queryNestedPath)
----------------------------------------------

``queryNestedPath`` on a ``SearchRequest`` scopes the entire search to a specific nested level, returning child documents directly as hits instead of their root parents.

.. code-block:: json

    {
        "indexName": "my_index",
        "queryNestedPath": "orders.items",
        "query": {
            "termQuery": {"field": "orders.items.item_name", "textValue": "widget"}
        },
        "retrieveFields": ["orders.items.item_name", "orders.items.quantity"]
    }

This returns one hit per matching item document, not per root document. ``queryNestedPath`` must be a field registered with ``nestedDoc: true``.

Script Access: _PARENT. and _CHILDREN.
----------------------------------------

Inside a script (e.g. a ``FunctionScoreQuery``), doc values from parent or child documents can be accessed using special field name prefixes.

**_PARENT.fieldName**

Reads a doc-values field from the **immediate parent** of the current document. Can be chained:

- From an item doc: ``_PARENT.orders.order_name`` → the order's name
- From an item doc: ``_PARENT._PARENT.root_field`` → the root document's field (two hops)
- From an order doc: ``_PARENT.root_field`` → the root document's field

This works because each child document carries a ``_parent_offset`` pointing to its immediate parent. Available in any nested-level script context.

**_CHILDREN.fieldName**

Aggregates doc-values from all immediate child documents of the current document. The parent boundary is derived from the **field's schema position** — specifically, the parent of the field's nested level — resolved once per field per segment and independent of query context.

For ``_CHILDREN.orders.items.quantity``:

- The field's nested level is ``orders.items`` (nearest ``nestedDoc`` ancestor).
- The parent of that level is ``orders``.
- The ``orders`` BitSet is used as the parent boundary regardless of whether the script runs in a root-level search, a ``queryNestedPath="orders"`` search, or inside a ``NestedQuery(path="orders")``.

This means the current document must be an **order document** (present in the ``orders`` BitSet) for ``_CHILDREN.orders.items.quantity`` to return any values. A root document is not in the ``orders`` BitSet, so calling ``_CHILDREN.orders.items.quantity`` from a root-level script always returns empty.

To access item quantities, evaluate the script at the orders level:

- Use ``queryNestedPath="orders"`` to score order documents directly, or
- Use ``NestedQuery(path="orders", scoreMode=SUM)`` wrapping a ``FunctionScoreQuery`` to aggregate order-level scores up to root.

For direct children of root (single-level nesting), ``_CHILDREN.`` works as expected from a root doc because the field's parent level is ``_root``:

- ``_CHILDREN.orders.order_name`` from a root doc → returns all order names for that root document (parent level of ``orders.order_name`` is ``_root``).

.. code-block:: python

    # FunctionScoreQuery script (pseudo-code) — must run at the orders level
    # (via queryNestedPath="orders" or inside NestedQuery(path="orders"))
    quantities = doc["_CHILDREN.orders.items.quantity"]
    return sum(quantities)
