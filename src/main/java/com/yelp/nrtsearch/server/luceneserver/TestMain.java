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
package com.yelp.nrtsearch.server.luceneserver;

import com.google.gson.Gson;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Rescorer;
import java.nio.file.Files;
import java.nio.file.Path;

public class TestMain {
  public static void main(String[] args) {
    try {
      Gson gson = new Gson();
      String content = Files.readString(Path.of("/Users/taoyu/code/nrtsearch/test_query_2"));
      Query.Builder builder = Query.newBuilder();
      JsonFormat.parser().merge(content, builder);
      System.out.println(
          builder
              .build()
              .getFunctionScoreQuery()
              .getQuery()
              .getBooleanQuery()
              .getMinimumNumberShouldMatch());
      int[] a = {1, 2};
      System.out.println(gson.toJson(1));

      Object b = 3.0;
      System.out.println(String.valueOf(b));

      /**
       * String content2 = Files.readString(Path.of("/Users/taoyu/code/nrtsearch/test_query_3"));
       * VirtualField.Builder builder2 = VirtualField.newBuilder();
       * JsonFormat.parser().merge(content2, builder2);
       */
      Rescorer.Builder rbuilder = Rescorer.newBuilder();
      String content4 = Files.readString(Path.of("/Users/taoyu/code/nrtsearch/test_query_4"));
      JsonFormat.parser().merge(content4, rbuilder);
      System.out.println(JsonFormat.printer().print(rbuilder.build()));
    } catch (Exception e) {
      System.out.println(e);
      throw new RuntimeException(e);
    }
  }
}
