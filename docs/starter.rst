Get Started
==========================

Docker Compose Setup
---------------------------
For docker compose setup of this example, please refer to Docker Compose section of this doc

Launch you local NRTSearch server
---------------------------
Follow :doc:`introduction` of this doc to launch the NRTSearch server


Create your NRTSearch index
---------------------------
Create Index
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

  # ./build/install/nrtsearch/bin/lucene-client createIndex --indexName  testIdx


Configure the index
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::


  # ./build/install/nrtsearch/bin/lucene-client settings -f settings.json
  # cat settings.json
    {         "indexName": "testIdx",
              "directory": "MMapDirectory",
              "nrtCachingDirectoryMaxSizeMB": 0.0,
              "indexMergeSchedulerAutoThrottle": false,
              "concurrentMergeSchedulerMaxMergeCount": 16,
              "concurrentMergeSchedulerMaxThreadCount": 8
    }

Register Fields
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block::

  # ./build/install/nrtsearch/bin/lucene-client registerFields -f registerFields.json
  # cat registerFields.json
    {         "indexName": "testIdx",
              "field":
              [
                      { "name": "doc_id", "type": "ATOM", "storeDocValues": true},
                      { "name": "vendor_name", "type": "TEXT" , "search": true, "store": true, "tokenize": true},
                      { "name": "license_no",  "type": "INT", "multiValued": true, "storeDocValues": true}
              ]
    }


Start Index
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block::

  # ./build/install/nrtsearch/bin/lucene-client startIndex -f startIndex.json
  # cat startIndex.json
    {
        "indexName" : "testIdx"
    }

Add documents to your NRTSearch index
---------------------------
.. code-block::

  # ./build/install/nrtsearch/bin/lucene-client addDocuments -i testIdx -f docs.csv -t csv
  # cat docs.csv
    doc_id,vendor_name,license_no
    0,first vendor,100;200
    1,second vendor,111;222

Query your NRTSearch server
---------------------------
.. code-block::

  # ./build/install/nrtsearch/bin/lucene-client search -f search.json
  # cat search.json
    {
        "indexName": "testIdx",
        "startHit": 0,
        "topHits": 100,
        "retrieveFields": ["doc_id", "license_no", "vendor_name"],
         "queryText": "vendor_name:first vendor"
    }


Java client Example
---------------------------
.. code-block::

  LuceneServerStubBuilder luceneServerStubBuilder = new LuceneServerStubBuilder("localhost", 8000);
  LuceneServerGrpc.LuceneServerBlockingStub blockingStub = luceneServerStubBuilder.createBlockingStub();
  SearchRequest request = SearchRequest.newBuilder()
    .setIndexName("testIdx")
    .setStartHit(0)
    .setTopHits(100)
    .addAllRetrieveFields(List.of("doc_id", "license_no", "vendor_name"))
    .setQueryText("vendor_name:first vendor")
    .build());
  blockingStub.search(request);
