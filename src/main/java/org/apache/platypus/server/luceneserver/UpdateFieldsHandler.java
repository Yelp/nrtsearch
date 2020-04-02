/*
 *
 *  *
 *  *  Copyright 2019 Yelp Inc.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  *  either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *
 *
 *
 */

package org.apache.platypus.server.luceneserver;

import org.apache.platypus.server.grpc.FieldDefRequest;
import org.apache.platypus.server.grpc.FieldDefResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateFieldsHandler implements Handler<FieldDefRequest, FieldDefResponse> {
    Logger logger = LoggerFactory.getLogger(UpdateFieldsHandler.class);

    @Override
    public FieldDefResponse handle(IndexState indexState, FieldDefRequest protoRequest) throws HandlerException {
        return new RegisterFieldsHandler().handle(indexState, protoRequest);
    }
}
