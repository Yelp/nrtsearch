InnerHit
==========================

Nested objects are stored as the separate documents, compared to the parent documents. NestedQuery enables the filter on parent documents that have at least one nested/child document matches the inner filters. However, the HitResponse will return only the parent documents, so no matched child information will be available from it. To get all the matched child documents per parent document, the innerHit must be used. Users may think the innerHit as a second layer search for each parent hit, and an empty innerHit query would return all children for each hit.

Requirements
------------

To start an innerHit, a parent searchRequest must be present. In additional, the index to search must have the child object field registed as nested field.

.. code-block:: json
    {
      "name": "field_name"
      "nestedDoc": true,
      "multiValued": true,
      "type": "OBJECT",
      "childFields": [
        ...
      ]
    }


Query Syntax
------------

This is the proto definition for InnerHit message which can be specified in SearchRequest:

.. code-block:: protobuf

    /* Inner Hit search request */
    message InnerHit {
        // Nested path to search against assuming same index as the parent Query.
        string query_nested_path = 1;
        // Which hit to start from (for pagination); default: 0
        int32 start_hit = 2;
        // How many top hits to retrieve; default: 3. It limits the hits returned, starting from index 0. For pagination: set it to startHit + window_size.
        int32 top_hits = 3;
        // InnerHit query to query against the nested documents specified by queryNestedPath.
        Query inner_query = 4;
        // Fields to retrieve; Parent's fields except its id field are unavailable in the innerHit.
        repeated string retrieve_fields = 5;
        // Sort hits by field (default is by relevance).
        QuerySortField query_sort = 6;
        // Highlight the children documents.
        Highlight highlight = 7;
    }


Example Queries
---------------

Assuming we have a yaml representation of the documents stored in `index_alpha`:

.. code-block:: yaml

  // parent document 1
  - business_name: restaurant_A
    business_address: 10 A street
    menu:
      - food_name: chicken
        price: 5
      - food_name: burger
        price: 8
  // parent document 2
  - business_name: restaurant_B
    business_address: 6 B avenue
    menu:
      - food_name: coke
        price: 4
      - food_name: cheeseburger
        price: 10

Case 1
^^^^^^
We would like to get all parents. - get all business. (no innerHit involvement)

.. code-block:: json

  {
    "indexName": "index_alpha",
    "retrieveFields": ["business_name"]
  }


Case 2
^^^^^^
We would like to get all children. - get all food in the menu for each business.

.. code-block:: json

  {
    "indexName": "index_alpha",
    "retrieveFields": ["business_name"],
    "innerHit": {
      "query_nested_path": "menu",
      "retrieve_fields": ["menu.food_name"]
    }
  }

Case 3
^^^^^^
We would like to get all children with parent filtering. - get all food in the menu for restaurant_A.

.. code-block:: json

  {
    "indexName": "index_alpha",
    "query": {
      "termQuery":{
        "field": "business_name",
        "textValue": "restaurant_A"
      }
    },
    "retrieveFields": ["business_name"],
    "innerHit": {
      "query_nested_path": "menu",
      "retrieve_fields": ["menu.food_name"]
    }
  }

Case 4
^^^^^^
We would like to get all children with child filtering. - get all food in the menu whose price is lower than 6.

.. code-block:: json

  {
    "indexName": "index_alpha",
    "retrieveFields": ["business_name"],
    "innerHit": {
      "query_nested_path": "menu",
      "query": {
        "rangeQuery":{
          "field": "menu.price",
          "upper": "6"
        }
      },
      "retrieve_fields": ["menu.food_name"]
    }
  }

Case 5
^^^^^^
We would like to get children with both parent and child filtering. - get all food in the menu whose price is lower than 6 within resturant_A.

.. code-block:: json

  {
    "indexName": "index_alpha",
    "query": {
      "termQuery":{
        "field": "business_name",
        "textValue": "restaurant_A"
      }
    },
    "retrieveFields": ["business_name"],
    "innerHit": {
      "query_nested_path": "menu",
      "query": {
        "rangeQuery":{
          "field": "menu.price",
          "upper": "6"
        }
      },
      "retrieve_fields": ["menu.food_name"]
    }
  }