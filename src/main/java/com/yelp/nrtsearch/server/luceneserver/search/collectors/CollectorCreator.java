/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.TermsCollectorManager;

/**
 * Helper class for creating instances of {@link AdditionalCollectorManager} from the grpc {@link
 * Collector} messages.
 */
public class CollectorCreator {

  private CollectorCreator() {}

  /**
   * Create {@link AdditionalCollectorManager} for the given {@link Collector} definition message.
   *
   * @param context search context
   * @param name collection name
   * @param collector collector definition message
   * @return collector manager usable for search
   */
  public static AdditionalCollectorManager<
          ? extends org.apache.lucene.search.Collector, ? extends CollectorResult>
      createCollectorManager(CollectorCreatorContext context, String name, Collector collector) {
    switch (collector.getCollectorsCase()) {
      case TERMS:
        return TermsCollectorManager.buildManager(name, collector.getTerms(), context);
      default:
        throw new IllegalArgumentException(
            "Unknown Collector type: " + collector.getCollectorsCase());
    }
  }
}
