FROM azul/zulu-openjdk-debian:14

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y wget unzip htop \
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
RUN ./gradlew clean installDist

RUN mkdir /go
ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"

#use go modules to pin module versions
ENV GO111MODULE=on
ENV GRPC_GATEWAY_VERSION=1.15.2
ENV PROTOC_GEN_GO_VERSION=1.5.1
ENV GRPC_VERSION=1.38.0

RUN go mod init github.com/Yelp/nrtsearch

#install required go protoc plugins to build grpc-gateway server
RUN go get \
    github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway@v${GRPC_GATEWAY_VERSION} \
    github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger@v${GRPC_GATEWAY_VERSION} \
    github.com/golang/protobuf/protoc-gen-go@v${PROTOC_GEN_GO_VERSION} \
    google.golang.org/grpc@v${GRPC_VERSION}

ENV PROTO_PATH=/code/clientlib/src/main/proto
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
$PROTO_PATH/yelp/nrtsearch/search.proto \
$PROTO_PATH/yelp/nrtsearch/suggest.proto

RUN protoc \
-I $PROTO_PATH \
-I $PROTO_BUILD_PATH/extracted-protos/main \
-I $PROTO_BUILD_PATH/extracted-include-protos/main \
--plugin=protoc-gen-grpc-gateway=$GOPATH/bin/protoc-gen-grpc-gateway \
--grpc-gateway_out=logtostderr=true:$OUTPATH \
$PROTO_PATH/yelp/nrtsearch/analysis.proto \
$PROTO_PATH/yelp/nrtsearch/luceneserver.proto \
$PROTO_PATH/yelp/nrtsearch/search.proto \
$PROTO_PATH/yelp/nrtsearch/suggest.proto

RUN protoc \
-I $PROTO_PATH \
-I $PROTO_BUILD_PATH/extracted-protos/main \
-I $PROTO_BUILD_PATH/extracted-include-protos/main \
--plugin=protoc-gen-swagger=$GOPATH/bin/protoc-gen-swagger \
--swagger_out=logtostderr=true:$OUTPATH \
$PROTO_PATH/yelp/nrtsearch/analysis.proto \
$PROTO_PATH/yelp/nrtsearch/luceneserver.proto \
$PROTO_PATH/yelp/nrtsearch/search.proto \
$PROTO_PATH/yelp/nrtsearch/suggest.proto

RUN cp $OUTPATH/yelp/nrtsearch/* grpc-gateway/
RUN cp $OUTPATH/github.com/Yelp/nrtsearch/* grpc-gateway/

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
