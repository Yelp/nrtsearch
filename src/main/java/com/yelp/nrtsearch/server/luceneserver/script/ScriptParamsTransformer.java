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
package com.yelp.nrtsearch.server.luceneserver.script;

import com.yelp.nrtsearch.server.grpc.Script;
import java.util.Objects;
import java.util.function.Function;

/**
 * Function to take the gRPC {@link com.yelp.nrtsearch.server.grpc.Script.ParamValue} included with
 * a {@link Script} and convert it to the equivalent simple java type.
 */
public class ScriptParamsTransformer
    implements com.google.common.base.Function<Script.ParamValue, Object>,
        Function<Script.ParamValue, Object> {
  public static final ScriptParamsTransformer INSTANCE = new ScriptParamsTransformer();

  @Override
  public Object apply(Script.ParamValue paramValue) {
    Objects.requireNonNull(paramValue);
    switch (paramValue.getParamValuesCase()) {
      case TEXTVALUE:
        return paramValue.getTextValue();
      case BOOLEANVALUE:
        return paramValue.getBooleanValue();
      case INTVALUE:
        return paramValue.getIntValue();
      case LONGVALUE:
        return paramValue.getLongValue();
      case FLOATVALUE:
        return paramValue.getFloatValue();
      case DOUBLEVALUE:
        return paramValue.getDoubleValue();
      case PARAMVALUES_NOT_SET:
        return null;
      default:
        throw new IllegalArgumentException(
            "Unknown script parameter type: " + paramValue.getParamValuesCase().name());
    }
  }
}
