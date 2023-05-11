Additional Collectors
==========================

The SearchRequest can define a mapping of additional collectors to compute aggregations over the recalled document set.

.. code-block::

    //Definition of additional document collector.
    message Collector {
        oneof Collectors {
            //Collector for aggregating based on term values.
            TermsCollector terms = 1;
            PluginCollector pluginCollector = 2;
            //Collector for getting top hits based on score or sorting.
            TopHitsCollector topHitsCollector = 4;
            //Collector that filters documents to nested collectors
            FilterCollector filter = 5;
            //Collector for finding a max double value from collected documents.
            MaxCollector max = 6;
        }
        //Nested collectors that define sub-aggregations per bucket, supported by bucket based collectors.
        map<string, Collector> nestedCollectors = 3;
    }

Terms Collector
-----------------------------
Aggregate term counts into buckets based on term values.

.. code-block::

    //Definition of term aggregating collector.
    message TermsCollector {
        oneof TermsSource {
            //Use field values for terms.
            string field = 1;
            //Use FacetScript definition to produce terms.
            Script script = 2;
        }
        //Maximum number of top terms to return.
        int32 size = 3;
        //How results Buckets should be ordered, defaults to descending Bucket _count.
        BucketOrder order = 4;
    }

Terms can either come from the doc values of an indexed field, or be produced by a script. If nestedCollectors are defined, they will compute sub-aggregations per bucket.

Bucket Order
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The ordering to use when sorting aggregated buckets can optionally be defined. The order defaults to the bucket count value in descending order.

.. code-block::

    //Defines how Buckets should be ordered in BucketResult.
    message BucketOrder {
        //Sorting order type
        enum OrderType {
            DESC = 0;
            ASC = 1;
        }
        //What to use for sorting. This can be _count for Bucket count, or the name of a nested collector that supports ordering.
        string key = 1;
        //Sorting order
        OrderType order = 2;
    }

When specifying a nested collector name, the collector type must support ordering. Currently, only the max collector type supports this.

Plugin Collector
-----------------------------
Aggregate using a collector defined by a plugin.

.. code-block::

    // Defines an entry point for using a collector from a plugin
    message PluginCollector {
        string name = 1;
        google.protobuf.Struct params = 2; // arguments passed to the plugin
    }

The name must have been registered by a plugin defining the CollectorPlugin interface.

Top Hits Collector
-----------------------------
Collect the top hits for aggregated documents based on relevance score or sorting.

.. code-block::

    //Definition of top hits based collector.
    message TopHitsCollector {
        //Offset for retrieval of top hits.
        int32 startHit = 1;
        //Total hits to collect, note that the number of hits returned is (topHits - startHit).
        int32 topHits = 2;
        //When specified, collector does sort based collection. Otherwise, relevance score is used.
        QuerySortField querySort = 3;
        //Which fields to retrieve.
        repeated string retrieveFields = 4;
        // If Lucene explanation should be included in the collector response
        bool explain = 5;
    }

Filter Collector
-----------------------------
Forwards documents through to nested collectors that pass the given criteria.

.. code-block::

    //Definition of filtering collector, there must be at least one nested collector specified in the Collector message.
    message FilterCollector {
        oneof Filter {
            // Only propagate documents that match the given query.
            Query query = 1;
            // Specialized implementation for set queries, checks if field doc values are in the provided set. This can be useful for large set sizes with lower recall, where building the scorer would be expensive.
            TermInSetQuery setQuery = 2;
        }
    }

Max Collector
-----------------------------
Collect a maximum double value across all aggregated documents. This value is produced by execution of a score script per document.

.. code-block::

    //Definition of collector to find a max double value over documents. Currently only allows for script based value production.
    message MaxCollector {
        oneof ValueSource {
            //Script to produce a double value
            Script script = 1;
        }
    }

This aggregation is usable for sorting buckets as a nested collector.