#!/bin/sh

HOSTNAME=$(hostname -i)

echo "hostname: "$HOSTNAME

echo "replacing nodeName"
sed -i "s/node-name/$HOSTNAME/g" docker-compose-config/nrtsearch_replica_config.yaml

echo "replacing nostname"
sed -i "s/host-name-replica/$HOSTNAME/g" docker-compose-config/nrtsearch_replica_config.yaml

echo "starting service"
/user/app/build/install/nrtsearch/bin/nrtsearch_server /user/app/docker-compose-config/nrtsearch_replica_config.yaml