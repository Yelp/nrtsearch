set -euxo pipefail
docker build -t grpc-gateway -f grpc-gateway/Dockerfile .
CID=$(docker run -d grpc-gateway)
docker cp $CID:/code/output/yelp/nrtsearch/. ./grpc-gateway/
docker cp $CID:/code/output/github.com/Yelp/nrtsearch/. ./grpc-gateway/
docker cp $CID:/code/bin/. ./build/install/nrtsearch/bin
docker container rm $CID