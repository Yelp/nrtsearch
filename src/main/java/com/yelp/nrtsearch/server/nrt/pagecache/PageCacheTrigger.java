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
package com.yelp.nrtsearch.server.nrt.pagecache;

import java.util.Locale;

/** What caused a page cache policy action to be applied, used to label metrics. */
public enum PageCacheTrigger {
  /** Files copied for a new nrt point on a replica. */
  NRT_POINT,
  /** Files pre-copied for a merged segment on a replica. */
  MERGE_PRECOPY,
  /** Existing or restored index files on startup. */
  STARTUP;

  private final String label = name().toLowerCase(Locale.ROOT);

  /** Get the metric label value. */
  public String label() {
    return label;
  }
}
