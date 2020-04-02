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

import com.yelp.nrtsearch.server.grpc.MetricFamilySamples;
import com.yelp.nrtsearch.server.grpc.MetricsResponse;
import com.yelp.nrtsearch.server.grpc.Sample;
import com.yelp.nrtsearch.server.grpc.SampleType;

import java.util.Enumeration;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

public class MetricsRequestHandler {
    private final CollectorRegistry collectorRegistry;

    public MetricsRequestHandler(CollectorRegistry collectorRegistry) {
        this.collectorRegistry = collectorRegistry;
    }

    public MetricsResponse process() {
        MetricsResponse.Builder metricsResponseBuilder = MetricsResponse.newBuilder();
        Enumeration<Collector.MetricFamilySamples> samples = collectorRegistry.metricFamilySamples();
        while (samples.hasMoreElements()) {
            metricsResponseBuilder.addMetricFamilySample(buildMetricFamilySamples(samples.nextElement()));
        }
        return metricsResponseBuilder.build();
    }

    private MetricFamilySamples buildMetricFamilySamples(Collector.MetricFamilySamples metricFamilySamples) {
        var metricFamilySamplesBuilder = MetricFamilySamples.newBuilder();
        for (Collector.MetricFamilySamples.Sample sample : metricFamilySamples.samples) {
            metricFamilySamplesBuilder.addSamples(buildSample(sample));
        }
        return metricFamilySamplesBuilder
                .setName(metricFamilySamples.name)
                .setHelp(metricFamilySamples.help)
                .setType(SampleType.valueOf(metricFamilySamples.type.name()))
                .build();
    }

    private Sample buildSample(Collector.MetricFamilySamples.Sample metricsSample) {
        return Sample.newBuilder()
                .setName(metricsSample.name)
                .addAllLabelNames(metricsSample.labelNames)
                .addAllLabelValues(metricsSample.labelValues)
                .setValue(metricsSample.value)
                .setTimestampMs(metricsSample.timestampMs!=null? metricsSample.timestampMs: 0)
                .build();
    }

}
