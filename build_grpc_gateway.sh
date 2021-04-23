set -euxo pipefail
docker build -t grpc-gateway -f grpc-gateway/Dockerfile .
docker run grpc-gateway
docker cp $(docker ps -alq):/code/output/yelp/nrtsearch/. ./grpc-gateway/
docker cp $(docker ps -alq):/code/output/github.com/Yelp/nrtsearch/. ./grpc-gateway/
docker cp $(docker ps -alq):/code/bin/. ./build/install/nrtsearch/bin
