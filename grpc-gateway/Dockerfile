FROM ubuntu:19.10

RUN apt-get update && \
    apt-get install -y wget unzip htop \
    openjdk-14-jdk \
    golang-go \
    git

# Install protoc
ENV PROTOC_VERSION=3.11.4
RUN wget https://github.com/google/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip \
    -O /protoc-${PROTOC_VERSION}-linux-x86_64.zip && \
    unzip /protoc-${PROTOC_VERSION}-linux-x86_64.zip -d /usr/local/ && \
    rm -f /protoc-${PROTOC_VERSION}-linux-x86_64.zip

RUN mkdir -p /code/output/
RUN mkdir -p /code/bin
ENV BASEPATH /code
ENV OUTPATH /code/output
ENV BINPATH /code/bin

COPY / ${BASEPATH}/
WORKDIR  $BASEPATH

ENV JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
RUN ./gradlew clean && ./gradlew installDist

RUN mkdir /go
ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
#install required go protoc plugins to build grpc-gateway server
RUN go get -u \
    github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway \
    github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger \
    github.com/golang/protobuf/protoc-gen-go \
    google.golang.org/grpc

ENV PROTO_PATH=/code/clientlib/proto
ENV PROTO_BUILD_PATH=/code/clientlib/build

#run protoc plugin for protoc-gen-go
RUN protoc \
-I $PROTO_PATH \
-I  $PROTO_BUILD_PATH/extracted-protos/main \
-I $PROTO_BUILD_PATH/extracted-include-protos/main \
--plugin=protoc-gen-go=$GOPATH/bin/protoc-gen-go \
--go_out=plugins=grpc:$OUTPATH \
$PROTO_PATH/yelp/nrtsearch/analysis.proto \
$PROTO_PATH/yelp/nrtsearch/luceneserver.proto \
$PROTO_PATH/yelp/nrtsearch/search.proto

RUN protoc \
-I $PROTO_PATH \
-I $PROTO_BUILD_PATH/extracted-protos/main \
-I $PROTO_BUILD_PATH/extracted-include-protos/main \
--plugin=protoc-gen-grpc-gateway=$GOPATH/bin/protoc-gen-grpc-gateway \
--grpc-gateway_out=logtostderr=true:$OUTPATH \
$PROTO_PATH/yelp/nrtsearch/analysis.proto \
$PROTO_PATH/yelp/nrtsearch/luceneserver.proto \
$PROTO_PATH/yelp/nrtsearch/search.proto

RUN protoc \
-I $PROTO_PATH \
-I $PROTO_BUILD_PATH/extracted-protos/main \
-I $PROTO_BUILD_PATH/extracted-include-protos/main \
--plugin=protoc-gen-swagger=$GOPATH/bin/protoc-gen-swagger \
--swagger_out=logtostderr=true:$OUTPATH \
$PROTO_PATH/yelp/nrtsearch/analysis.proto \
$PROTO_PATH/yelp/nrtsearch/luceneserver.proto \
$PROTO_PATH/yelp/nrtsearch/search.proto

RUN cp $OUTPATH/yelp/nrtsearch/* grpc-gateway/

# build go executables for various platforms
RUN for GOOS in darwin linux windows; do \
    for GOARCH in 386 amd64; do \
        echo "Building $GOOS-$GOARCH"; \
        export GOOS=$GOOS; \
        export GOARCH=$GOARCH; \
        go build -o bin/http_wrapper-$GOOS-$GOARCH http_wrapper.go; \
    done; \
done

CMD /bin/sh
