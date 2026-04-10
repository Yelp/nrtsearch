/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.query;

import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.state.GlobalState;
import javax.annotation.Nullable;

/**
 * Context for query building. Bundles {@link DocLookup} (for field resolution and doc values) with
 * optional {@link GlobalState} (needed for cross-index queries like {@link
 * com.yelp.nrtsearch.server.grpc.CrossIndexQuery}).
 */
public record QueryContext(DocLookup docLookup, @Nullable GlobalState globalState) {}
