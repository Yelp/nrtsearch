Docker Compose
==========================

Introduction
-----------------------------

The Docker Compose version allows for local testing and serves as an example.  It is not meant to be an example of how to run nrtSearch in production.

This document shows a step-by-step way to create indexes, insert documents, commit them on the primary, and have them replicated to replica nodes.

Since it does not include persistent storage in S3 that cannot be tested, but one can create indexes, insert data, and still do searches (without actually having a valid S3 endpoint).

This version has been tested with docker version and docker-compose version:

.. code-block::

  shell$ docker -v
  Docker version 20.10.8, build 3967b7d28e

  shell$ docker-compose -v
  docker-compose version 1.27.4, build 40524192

1. Starting Containers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is a Dockerfile in the main directory, which is used by docker-compose to build the image used for both the primary and replica nodes.   Currently there are 3 replicas started, but that can be increased in the docker-compose.yaml file.

.. code-block::

  shell% docker-compose -f docker-compose.yaml up

Indexing and Replication
"""""""""""""""""""""""""""

There are configuration files, and one data file, under the sub-directory docker-compose-config.

At the start of containers, no index is created. The index has to be started, documents entered in the primary, and commited.   When the replicas are started, they register with the primary. After the primary commits the documents, they are synced with the replicas. A step-by-step example is below.

2. Primary: Start Index and Add Documents
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start primary index, and add docs for indexing:

.. code-block::

  shell% PRIMARY_CONTAINER_ID=$(docker ps | grep nrtsearch_primary-node | awk '{print $1}')
  shell% docker exec -it $PRIMARY_CONTAINER_ID sh
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 createIndex --indexName  testIdx
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 settings -f docker-compose-config/settings_primary.json
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 registerFields -f docker-compose-config/registerFields.json
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 startIndex -f docker-compose-config/startIndex_primary.json
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 addDocuments -i testIdx -f docker-compose-config/docs.csv -t csv

3. Replica: Start Index
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Next go into any one of the replicas (i.e. nrtsearch_replica-node-1 here), and run the commands to start the index and register with the primary.  Do **not** enter docs:

.. code-block::

  shell% REPLICA_1_CONTAINER_ID=$(docker ps  | grep nrtsearch_replica-node-1_1 | awk '{print $1}')
  shell% docker exec -it $REPLICA_1_CONTAINER_ID sh
  # ./build/install/nrtsearch/bin/lucene-client -h replica-node-1 -p 8002 createIndex --indexName  testIdx
  # ./build/install/nrtsearch/bin/lucene-client -h replica-node-1 -p 8002 settings -f docker-compose-config/settings_replica.json
  # ./build/install/nrtsearch/bin/lucene-client -h replica-node-1 -p 8002 registerFields -f docker-compose-config/registerFields.json
  # ./build/install/nrtsearch/bin/lucene-client -h replica-node-1 -p 8002 startIndex -f docker-compose-config/startIndex_replica.json

4. Primary: Commit Documents
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Commit the documents on the Primary

.. code-block::

  shell% docker exec -it $PRIMARY_CONTAINER_ID sh
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 commit -i testIdx

5. Replica: Search Should Work
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The search should now work on any of the replicas where the the index was started and registered on the primary.

.. code-block::

  shell% docker exec -it $REPLICA_1_CONTAINER_ID sh
  # ./build/install/nrtsearch/bin/lucene-client -h replica-node-1 -p 8002 search -f docker-compose-config/search.json
  ...
  fields {
    key: "license_no"
    value {
      fieldValue {
        intValue: 111
      }
      fieldValue {
        intValue: 222
      }
    }
  }

Logging
"""""""""""""""""""""""""""

To view the logs in the containers use docker-compose logs:

.. code-block::

  shell% docker-compose logs
  replica-node-1_1  | [INFO ] 2021-12-13 18:58:26.527 [main] LuceneServer - Server started, listening on 8003 for replication messages
  replica-node-1_2  | hostname: 172.24.0.2
  primary-node      | [INFO ] 2021-12-13 18:58:28.530 [main] LuceneServer - Server started, listening on 8001 for replication messages

Stop
"""""""""""""""""""""""""""

To stop all the containers use docker-compose

.. code-block::

  shell% docker-compose down
  Stopping nrtsearch_replica-node-1_3 ... done
  Stopping nrtsearch_replica-node-1_1 ... done
  Stopping nrtsearch_replica-node-1_2 ... done
  Stopping primary-node               ... done
  Removing nrtsearch_replica-node-1_3 ... done
  Removing nrtsearch_replica-node-1_1 ... done
  Removing nrtsearch_replica-node-1_2 ... done
  Removing primary-node               ... done
  Removing network nrtsearch_default
