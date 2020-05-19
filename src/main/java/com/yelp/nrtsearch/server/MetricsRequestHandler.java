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

package com.yelp.nrtsearch.server;

import com.google.api.HttpBody;
import com.google.protobuf.ByteString;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class MetricsRequestHandler {
    private final CollectorRegistry collectorRegistry;

    public MetricsRequestHandler(CollectorRegistry collectorRegistry) {
        this.collectorRegistry = collectorRegistry;
    }

    public HttpBody process() throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             OutputStreamWriter outputStreamWriter = new OutputStreamWriter(byteArrayOutputStream);
             Writer writer = new BufferedWriter(outputStreamWriter)) {
            TextFormat.write004(writer, collectorRegistry.metricFamilySamples());
            writer.flush();
            return HttpBody.newBuilder()
                    .setContentType("text/plain")
                    .setData(ByteString.copyFrom(byteArrayOutputStream.toByteArray()))
                    .build();
        }
    }

}
