/*
 *
 *  * Copyright 2019 Yelp Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  * either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.platypus.server.luceneserver;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.stub.StreamObserver;
import org.apache.platypus.server.grpc.ReplicationServerClient;

/* Interface for handlers that take in an indexState and the protoBuff request and returns the protoBuff response */
public interface Handler<T extends GeneratedMessageV3, S extends GeneratedMessageV3> {

    default boolean isValidMagicHeader(int magicHeader) {
        return magicHeader == ReplicationServerClient.BINARY_MAGIC;
    }

    default void handle(IndexState indexState, T protoRequest, StreamObserver<S> responseObserver) throws Exception {
        throw new UnsupportedOperationException("This method is not supported");
    }

    S handle(IndexState indexState, T protoRequest) throws HandlerException;

    class HandlerException extends Exception {
        public HandlerException(Throwable err) {
            super(err);
        }

        public HandlerException(String errorMessage) {
            super(errorMessage);
        }

        public HandlerException(String errorMessage, Throwable err) {
            super(errorMessage, err);
        }

    }

}
