/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.grpc.discovery;

import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.NameResolver;
import java.net.URI;
import org.junit.Test;

public class PrimaryFileNameResolverProviderTest {
  @Test
  public void testProvidesResolver() {
    PrimaryFileNameResolverProvider provider =
        new PrimaryFileNameResolverProvider(new ObjectMapper(), 1, 1000);
    NameResolver resolver = provider.newNameResolver(URI.create("/some/file/path"), null);
    assertTrue(resolver instanceof PrimaryFileNameResolver);
  }
}
