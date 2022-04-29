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
package com.yelp.nrtsearch.server.luceneserver.suggest.protocol;

import java.util.List;

/**
 * context suggest field data used to create a context suggest field
 *
 * @param value field values to get suggestions on
 * @param contexts contexts associated with the value
 * @param weight field weight
 */
public class ContextSuggestFieldData {
  private String value;
  private List<String> contexts;
  private int weight;

  public ContextSuggestFieldData(String value, List<String> contexts, int weight) {
    this.value = value;
    this.contexts = contexts;
    this.weight = weight;
  }

  public String getValue() {
    return this.value;
  }

  public List<String> getContexts() {
    return this.contexts;
  }

  public int getWeight() {
    return this.weight;
  }
}
