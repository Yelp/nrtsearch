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
package com.chase.app.search.contracts;

import java.util.Arrays;
import java.util.stream.Stream;

public class SearchFilter {
    public String[] appId;
    public String[] resourceType;
    public String[] link;

    public boolean hasAny()
    {
        return (appId != null && appId.length > 0) ||
        (resourceType != null && resourceType.length > 0) ||
        (link != null && link.length > 0);
    }

    public SearchFilter clone(){
        SearchFilter cp = new SearchFilter();
        cp.appId = this.appId;
        cp.link = this.link;
        cp.resourceType = this.resourceType;
        return cp;
    }

    private String[] mergeArrays(String[] a1, String[] a2)
    {
        if (a2 != null)
        {
            return a1 == null ? a2 : Stream.of(a1, a2).flatMap(x -> Arrays.stream(x)).distinct().toArray(String[]::new);
        }
        else return a1;
    }
    public SearchFilter merge(SearchFilter another)
    {
        this.appId = mergeArrays(this.appId, another.appId);
        this.resourceType = mergeArrays(this.resourceType, another.resourceType);
        this.link = mergeArrays(this.link, another.link);
        return this;
    }
}