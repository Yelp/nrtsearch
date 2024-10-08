FROM debian:12

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y wget unzip htop \
    golang-go \
    git \
    gnupg2 software-properties-common

RUN wget -O- https://apt.corretto.aws/corretto.key | apt-key add -
RUN add-apt-repository -y 'deb https://apt.corretto.aws stable main'
# For some reason, needs to be run again for the repo to be usable
RUN add-apt-repository -y 'deb https://apt.corretto.aws stable main'
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y java-21-amazon-corretto-jdk

# Install protoc
ENV PROTOC_VERSION=24.3
RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip \
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
ENV GRPC_GATEWAY_VERSION=2.10.0
ENV PROTOC_GEN_GO_VERSION=1.5.2
ENV GRPC_VERSION=1.46.0

RUN go mod init github.com/Yelp/nrtsearch

#install required go protoc plugins to build grpc-gateway server
RUN go get \
    github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v${GRPC_GATEWAY_VERSION} \
    github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v${GRPC_GATEWAY_VERSION} \
    github.com/golang/protobuf/protoc-gen-go@v${PROTOC_GEN_GO_VERSION} \
    google.golang.org/grpc@v${GRPC_VERSION}

RUN go install \
    github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway \
    github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2
RUN go install github.com/golang/protobuf/protoc-gen-go@v${PROTOC_GEN_GO_VERSION}

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
--plugin=protoc-gen-openapiv2=$GOPATH/bin/protoc-gen-openapiv2 \
--openapiv2_out=logtostderr=true:$OUTPATH \
$PROTO_PATH/yelp/nrtsearch/analysis.proto \
$PROTO_PATH/yelp/nrtsearch/luceneserver.proto \
$PROTO_PATH/yelp/nrtsearch/search.proto

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
