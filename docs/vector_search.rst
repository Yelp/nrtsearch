Vector Search and Embeddings in Nrtsearch
==========================================

What is Vector/Embeddings?
--------------------------

Vector/embeddings are a simple data structure consisting of a list of (float) numbers with N dimensions to represent a variety of complex data such as photos, videos, textual data, etc., that cannot be stored in traditional databases or datastores in a searchable way. By utilizing this data structure, search engines that support vector search can find similar documents using KNN (K nearest neighbor) or ANN (approximate nearest neighbor) algorithms.

For example, one can search for photos using textual descriptions such as *"vegetarian pizza."* To achieve this, we need to first convert a large number of photos into vector embeddings, such as::

    [[1.0, 0.3, 2.1], [0.01, 0.02, 1.24], ...]

using an appropriate algorithm and store them in a database that supports vector search. Subsequently, users' textual queries will be transformed into vector embeddings, such as::

    [1.0, 3.2, 1.3]

The algorithm will then determine the most relevant photos by traversing through a graph and applying similarity algorithms such as cosine similarity, dot product, or others.

You can find more detailed information about vector embeddings in the following articles:

- `Google <https://cloud.google.com/blog/topics/developers-practitioners/meet-ais-multitool-vector-embeddings>`_
- `Elastic <https://www.elastic.co/what-is/vector-embedding>`_

Vector Search Applications
--------------------------

The following is a list of example applications that can benefit from vector search:

- Photo search
- Review search
- Similar businesses search
- Bot detection
- Chatbots

Vector Embeddings on Nrtsearch
------------------------------

Nrtsearch is based on the popular open-source search library, Lucene. Vector search support was added to Lucene in version 9+. It uses the Hierarchical Navigable Small Worlds (HNSW) graph to traverse vector data to find the most relevant results. It supports vector-only search as well as hybrid (vector + text search).

More information about Lucene's support of vector search can be found at:

- `Lucene Vector Search <https://www.apachecon.com/acna2022/slides/04_lucene_vector_search_sokolov.pdf>`_

In the following sections, we will go through different steps to launch and configure an Nrtsearch cluster with vector search support.

Launching Cluster with Vector Search Support
--------------------------------------------

Estimate Cluster Size
^^^^^^^^^^^^^^^^^^^^^

Before launching a cluster, you need to figure out how big the data will be.

The general guidelines to calculate the size introduced by embeddings is to use the following formula::

    Size in bytes = Total Num docs * Float Size * (Embeddings dimensions + hnsw additional storage)

For example, if your index will have around 390M docs with embeddings of 512 dimensions, the total size for this cluster will be around 817GB::

    => ~390M * 4 * (512 + 12) = ~817GB

Note that the above formula is only for vector fields. Other fields in the index will require additional space.

Configuring Cluster
^^^^^^^^^^^^^^^^^^^

To use vector search, you need to add `VECTOR` type field to your index your create a new index with vector fields.
Example vector field definition::

    # Under field property
    - name: <VECTOR FIELD NAME>
      type: VECTOR
      search: true
      vectorSimilarity: cosine  # You need to reindex data if this value is changed.
      vectorIndexingOptions:
        - type: hnsw
          hnsw_m: 16  # The number of neighbors each node will be connected to in the HNSW graph, default: 16
          hnsw_ef_construction: 100  # The number of candidates to track while assembling the list of nearest neighbors for each new node, default: 100
      vectorDimensions: 512

* Lower `hnsw_m` and `hnsw_ef_construction` values can help with lowering latencies and improving indexing throughput. However, they affect search accuracy.
* Lower values for `vectorDimensions` help with less disk space and indexing throughput. However, it affects search accuracy.
* The maximum possible value for vectorDimensions in Nrtsearch is 4096

Nrtsearch also supports vector fields which don't generate a graph. For such existing fields, search can be set to False, if it's not needed in filtering.

Ingestion
^^^^^^^^^

You need to add the embeddings using AddDocumentRequests. The embeddings should be added as a jsonified list of floats in the `value` field. The number of floats should match the number of dimensions specified in the vector field definition.
Example request in json format::

    {
      "indexName": "vector_test",
      "fields": [
        {
          "field": "photo_id",
          "intValue": 1
        },
        {
          "field": "photo_embeddings",
          "value": "[0.188423157, 0.246743672, ...]"
        }
      ]
    }

You can also use lucene-client to load the documents with vector fields. Example csv input file:

    photo_id,photo_embeddings
    1,"[0.188423157, 0.246743672, ...]"
    2,"[0.188423157, 0.246743672, ...]"
    3,"[0.188423157, 0.246743672, ...]"

Search
------

KNN search can be configured using the following syntax::

    message KnnQuery {
      string field = 1;
      Query filter = 2;
      int32 k = 3;
      int32 num_candidates = 4;
      repeated float query_vector = 5;
      float boost = 6;
    }

You can perform three types of searches using KNN:

1. Vector-only search
2. Hybrid (vector + additional filter (text, term match, etc.)) Search + inline filter
3. Hybrid (vector + additional filter (text, term match, etc.)) Search + top-level filter

Vector-only search
^^^^^^^^^^^^^^^^^^
Vector only searches are straightforward and they do the look up through the graph. The lookups are fast and accurate. The level of latency and accuracy will depend on the graph configs and the number of vector hits.
Example::

    {
      "indexName": "vector_test",
      "startHit": 0,
      "topHits": 10,
      "timeoutSec": 0,
      "retrieveFields": ["photo_id", "business_id", "caption"],
      "knn": [
        {
          "field": "photo_embeddings",
          "k": 3,
          "num_candidates": 1000,
          "query_vector": [0.188423157, 0.246743672, ...]
        }
      ]
    }

Vector Search + Inline Filter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Nrtsearch doesn’t go through the HNSW graph at all. Instead it first filters data using the provided filter. Then it uses a KNN algorithm such as cosine similarity to score returned embeddings. While this option doesn’t traverse through the HNSW graph, it can return very accurate results. It will perform much better when the number of filtered docs to rank is less.
Example::

    {
      "indexName": "vector_test",
      "startHit": 0,
      "topHits": 10,
      "timeoutSec": 0,
      "retrieveFields": [
        "photo_id",
        "business_id",
        "caption"
      ],
      "knn": [
        {
          "field": "photo_embeddings",
          "k": 1,
          "num_candidates": 10,
       "filter": {
            "booleanQuery": {
              "clauses": [
              {
                  "occur": "MUST",
                  "query": {
                    "termQuery": {
                      "field": "business_id",
                      "intValue": 12581436
                    }
                  }
                }
              ]
            }
          },
          "query_vector": [
            0.188423157,
        ....
       ]
      }
     ]
    }

Vector Search + Top Level Filter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Here, Nrtsearch will try to find the best matching documents by traversing through the HNSW graph. At the same time it tries to find all the docs matching the top level query clause as well. Then it combines the results using an OR operator. While this approach is using the graph, it’s only recommended for specific use cases, as it won’t provide accurate results. For instance, if one is looking for the most similar photo in a particular business for the given photo, Nrtsearch will find top N photos that are very similar to the given photo across all businesses. It will try to pick the photos that belong to the given business as well. If some or none of the photos from the latter queries are found in the former one, then those photos will be still included in the final results, even though they may not be similar to the photo we are looking for.
Example::

    {
      "indexName": "vector_test",
      "startHit": 0,
      "topHits": 10,
      "timeoutSec": 0,
      "retrieveFields": [
        "photo_id",
        "business_id",
        "caption"
      ],
      "query": {
        "booleanQuery": {
          "clauses": [
            {
              "occur": "MUST",
              "query": {
                "termQuery": {
                  "field": "business_id",
                  "intValue": 12581436
                }
              }
            }
          ]
        }
      },
      "knn": [
        {
          "field": "photo_embeddings",
          "k": 100,
          "num_candidates": 1000,
          "query_vector": [
            0.188423157,
            -0.0844727457,
        ....
       ]
      }
     ]
    }

Another example where this particular use case may make sense is a scenario where one would want to find burger photos for a particular business. The vector search query can find its top burger photo across all businesses. The text search can apply a filter based on business ID and caption field of the document. If there are photos from the same business in the vector search, their score can be boosted using the boost parameter so that when combined with the regular text search results, they get higher score. In this case even if no photos are found from the vector search, the text search can at least show some photos whose caption matches the keyword "burger".

Optimizing Search Queries
-------------------------

The vector hits value represents the number of documents traversed during the vector search. It is the number of vector comparisons, which is the major factor in query performance. It plays the most important role in terms of search latencies and accuracy.
Any change that reduces the vector hits number, will decrease the latencies in expense of reducing accuracy.

A summary of trade-offs for each config:

* Improve Search Latency

  * Lower `num_candidates`

    * Lower vector hits
    * Lower accuracy

  * Lower indexing parameter values (`hnsw_m`, `hnsw_ef_construction`, `vectorDimensions`)

    * Lower vector hits
    * Lower accuracy
    * Lower indexing work
    * Requires reindexing

* Improve Indexing Throughput

  * Lower indexing parameter values (`hnsw_m`, `hnsw_ef_construction`, `vectorDimensions`)

    * Lower vector hits
    * Lower accuracy
    * Lower indexing work
    * Requires reindexing

* Improve Accuracy

  * Higher `num_candidates`

    * Higher vector hits
    * Higher accuracy

  * Higher indexing parameter values (`hnsw_m`, `hnsw_ef_construction`, `vectorDimensions`)

    * Higher vector hits
    * Higher accuracy
    * Higher indexing work
    * Requires reindexing
