docker build -t grpc-gateway -f grpc-gateway/Dockerfile .
docker run grpc-gateway
docker cp $(docker ps -alq):/code/output/. ./grpc-gateway/
docker cp $(docker ps -alq):/code/bin/. ./build/install/platypus/bin
