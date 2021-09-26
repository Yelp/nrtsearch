/*
 * Copyright 2020 Chase Labs Inc.
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

package com.chase.app.tokenizers;

import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.AttributeFactory;

// like https://github.com/chaseappio/backend-resources-service/blob/dev/src/Chase.Resources.Elastic/Template/Tokenizers/PathTokenizer.cs
public class PathsTokenizer extends CharTokenizer {
    public PathsTokenizer() {
        super();
    }

    public PathsTokenizer(AttributeFactory factory) {
        super(factory);
    }


    @Override
    protected boolean isTokenChar(int arg0) {
        char c = (char) arg0;
        return !(Character.isWhitespace(c) || c == ':' || c == '.' || c == '/' || c == '-' || c == '_');
    }
}