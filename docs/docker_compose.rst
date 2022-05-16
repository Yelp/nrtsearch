Docker Compose
==========================

Introduction
-----------------------------

The Docker Compose version allows for testing and serves as an example.  It is not meant to be an example of how to run nrtSearch in production.

This document shows a step-by-step way to create indexes, insert documents, commit them on the primary, and have them replicated to replica nodes.

Indexes are stored on an S3 bucket that must be crated before hand.  The S3 storage is currently accessible only from an EC2 instance.

This has been tested with docker and docker-compose versions:

.. code-block::

  shell$ docker -v
  Docker version 20.10.8, build 3967b7d28e

  shell$ docker-compose -v
  docker-compose version 1.27.4, build 40524192 # version 1.27 so the deploy.replicas option works

0. PreSteps
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start an AWS EC2 instance, with docker and git installed.  One needs docker-compose to run the example, however EC2 instances do not come with docker-compose, so that will need to be installed. One will also need a boto.cfg that can access a pre-existing S3 bucket.  This can be done like this (details may vary according to your AMI image):

.. code-block::

  # install docker-compose
  shell% sudo curl -L "https://github.com/docker/compose/releases/download/1.27.4/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
  shell% sudo chmod +x /usr/local/bin/docker-compose
  # set the AWS credentials in your boto.cfg file, which must be located in the project root directory.
  shell% cat boto.cfg
  [default]
  region = eu-central-1
  aws_access_key_id = A--------QA
  aws_secret_access_key = 7--------A
  http_socket_timeout = 300
  ec2_region_name = eu-central-1
  ec2_region_endpoint = ec2.eu-central-1.amazonaws.com

Note: The S3 bucket must exist before hand, and is named in the server configuration files.  For this example the S3 bucket in this example is named 'nrtsearch-bucket':

.. code-block::

  shell% cat docker-compose-config/lucene_server_configuration_primary.yaml # for the replica config as well
  ...
  bucketName: "nrtsearch-bucket"
  ...

1. Starting Containers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is a Dockerfile in the main directory, which is used by docker-compose to build the image used for both the primary and replica nodes. Currently there are 3 replicas started, but that can be increased in the docker-compose.yaml file.

.. code-block::

  shell% docker-compose -f docker-compose.yaml up

Indexing and Replication
"""""""""""""""""""""""""""

There are configuration files, and one data file, under the sub-directory docker-compose-config.

When the containers are started, no index is created. The index has to be started, and documents entered in the primary. When the replicas are started, they register with the primary. Replication happens after documents are added to the primary. A step-by-step example is below.

2. Primary: Start Index
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start primary index, and add docs for indexing:

.. code-block::

  shell% PRIMARY_CONTAINER_ID=$(docker ps | grep nrtsearch_primary-node | awk '{print $1}')
  shell% docker exec -it $PRIMARY_CONTAINER_ID sh
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 createIndex --indexName  testIdx
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 settings -f docker-compose-config/settings_primary.json
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 registerFields -f docker-compose-config/registerFields.json
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 startIndex -f docker-compose-config/startIndex_primary.json

3. Replica: Start Index
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Next go into any one of the replicas (i.e. nrtsearch_replica-node-1 here), and run the commands to start the index and register with the primary.  There is no need to add the documents in the replica.

.. code-block::

  shell% REPLICA_1_CONTAINER_ID=$(docker ps  | grep nrtsearch_replica-node-1_1 | awk '{print $1}')
  shell% docker exec -it $REPLICA_1_CONTAINER_ID sh
  # ./build/install/nrtsearch/bin/lucene-client -h replica-node-1 -p 8002 createIndex --indexName  testIdx
  # ./build/install/nrtsearch/bin/lucene-client -h replica-node-1 -p 8002 settings -f docker-compose-config/settings_replica.json
  # ./build/install/nrtsearch/bin/lucene-client -h replica-node-1 -p 8002 registerFields -f docker-compose-config/registerFields.json
  # ./build/install/nrtsearch/bin/lucene-client -h replica-node-1 -p 8002 startIndex -f docker-compose-config/startIndex_replica.json

4. Replication
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Search will work on replicas soon after documents are added on the primary.

.. code-block::

  shell% docker exec -it $PRIMARY_CONTAINER_ID sh
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 addDocuments -i testIdx -f docker-compose-config/docs.csv -t csv

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


6. Backup Index to S3
^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to backup the index, one can use the 'backupIndex' command with these parameters:

.. code-block::

  shell% docker exec -it $PRIMARY_CONTAINER_ID sh
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 backupIndex  --indexName testIdx --serviceName nrtsearch-service-test --resourceName testIdx

Now the S3 bucket 'nrtsearch-bucket' should contain the service 'nrtsearch-service-test' data :

.. code-block::

  shell% aws s3 ls nrtsearch-bucket/nrtsearch-service-test/
  .     PRE _version/
  .     PRE testIdx_data/
  .     PRE testIdx_metadata/

7. Restart nrtSearch with Backup Index on S3
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To demonstrate how one can start nrtSearch and restore the index data from S3, one has to change the lines in the following 4 config files.  They are restoring the state and providing the names in S3:

.. code-block::

  # update the 2 lucene service configs docker-compose-config/lucene_server_configuration_{primary,replica}.yaml to have this line:
  shell% cat docker-compose-config/lucene_server_configuration_primary.yaml
  ...
  # previous lines still there, change this line:
  restoreState: True
  ...
  ...
  shell% cat docker-compose-config/lucene_server_configuration_replica.yaml
  ...
  # previous lines still there, change this line:
  restoreState: True
  ...
  ...
  # and also add the restore JSON object in the 2 startIndex JSON config files with the correct service and resource names:
  shell% cat docker-compose-config/startIndex_primary.json
  ...
  # previous lines still there, new lines:
    "restore": {
      "serviceName": "nrtsearch-service-test",
      "resourceName": "testIdx",
      "deleteExistingData": false
    }
  }
  ...
  shell% cat docker-compose-config/startIndex_replica.json
  ...
  # previous lines still there, new lines:
    "restore": {
      "serviceName": "nrtsearch-service-test",
      "resourceName": "testIdx",
      "deleteExistingData": false
    }
  }

If one then restarts the containers and index (do not need to register the fields), then the search in Step 5 above should work, even though no documents were indexed (skipping Step 4). This means that the index was correctly loaded from the S3 bucket on startup.

.. code-block::

  shell% docker-compose down
  sehll% docker images | grep nrtsearch | awk '{print "docker rmi -f "$3}' | sh  # need to rebuild the images with the new config
  shell% docker-compose -f docker-compose.yaml up
  shell% PRIMARY_CONTAINER_ID=$(docker ps | grep nrtsearch_primary-node | awk '{print $1}')
  shell% docker exec -it $PRIMARY_CONTAINER_ID sh
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 startIndex -f docker-compose-config/startIndex_primary.json
  # ./build/install/nrtsearch/bin/lucene-client -h primary-node -p 8000 search -f docker-compose-config/search.json
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
