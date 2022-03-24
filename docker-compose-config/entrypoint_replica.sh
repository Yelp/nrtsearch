#!/bin/sh

HOSTNAME=$(hostname -i)

echo "hostname: "$HOSTNAME

echo "replacing nodeName"
sed -i "s/node-name/$HOSTNAME/g" docker-compose-config/lucene_server_configuration_replica.yaml

echo "replacing nostname"
sed -i "s/host-name-replica/$HOSTNAME/g" docker-compose-config/lucene_server_configuration_replica.yaml

echo "starting service"
/user/app/build/install/nrtsearch/bin/lucene-server /user/app/docker-compose-config/lucene_server_configuration_replica.yaml