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
package com.yelp.nrtsearch.server.leader;

import java.io.IOException;

/**
 * Thrown when a remote state write is rejected because this node is not, or is no longer, the
 * active primary. Seeing this exception means a newer leader has claimed the state, or this node
 * has not been promoted.
 */
public class FencedException extends IOException {
  public FencedException(String message) {
    super(message);
  }

  public FencedException(String message, Throwable cause) {
    super(message, cause);
  }
}
