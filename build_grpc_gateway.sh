docker build -t grpc-gateway -f grpc-gateway/Dockerfile .
status=$?
if [ $status -ne 0 ];
then
  echo "Unable to build grpc-gateway";
  exit $status;
fi
docker run grpc-gateway
docker cp $(docker ps -alq):/code/output/yelp/nrtsearch/. ./grpc-gateway/
docker cp $(docker ps -alq):/code/bin/. ./build/install/nrtsearch/bin
