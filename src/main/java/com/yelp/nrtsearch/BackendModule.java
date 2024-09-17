/*
 * Copyright 2021 Yelp Inc.
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
package com.yelp.nrtsearch;

import com.amazonaws.services.s3.AmazonS3;
import com.google.inject.*;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;

public class BackendModule extends AbstractModule {

  @Inject
  @Singleton
  @Provides
  protected RemoteBackend providesRemoteBackend(
      LuceneServerConfiguration configuration, AmazonS3 s3) {
    return new S3Backend(configuration, s3);
  }
}
