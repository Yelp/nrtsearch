/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.utils;

import com.yelp.nrtsearch.server.grpc.AutoFuzziness;
import org.apache.lucene.index.Term;

public class QueryUtils {
  private static final int DEFAULT_LOW = 3; // default low value for auto fuzziness
  private static final int DEFAULT_HIGH = 6; // default high value for auto fuzziness

  public static int computeMaxEditsFromTermLength(Term term, AutoFuzziness autoFuzziness) {
    int maxEdits;
    int low = autoFuzziness.getLow();
    int high = autoFuzziness.getHigh();
    int termLength = term.bytes().length;
    // If both values are not set, use default values
    if (low == 0 && high == 0) {
      low = DEFAULT_LOW;
      high = DEFAULT_HIGH;
    }
    if (low < 0) {
      throw new IllegalArgumentException("AutoFuzziness low value cannot be negative");
    }
    if (low >= high) {
      throw new IllegalArgumentException("AutoFuzziness low value should be < high value");
    }
    if (termLength >= 0 && termLength < low) {
      maxEdits = 0;
    } else if (termLength >= low && termLength < high) {
      maxEdits = 1;
    } else {
      maxEdits = 2;
    }
    return maxEdits;
  }
}
