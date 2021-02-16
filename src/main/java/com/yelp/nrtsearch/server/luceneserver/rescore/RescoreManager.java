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
package com.yelp.nrtsearch.server.luceneserver.rescore;

import com.yelp.nrtsearch.server.grpc.QueryRescorer;
import com.yelp.nrtsearch.server.grpc.ScriptRescorer;
import com.yelp.nrtsearch.server.grpc.SearchRequest.Rescorer;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

public class RescoreManager {

  private static final QueryNodeMapper queryNodeMapper = new QueryNodeMapper();

  public static TopDocs rescore(
      List<Rescorer> rescorers,
      IndexSearcher searcher,
      TopDocs firstPassTopDocs,
      IndexState indexState)
      throws IOException {

    TopDocs hits = firstPassTopDocs;

    for (Rescorer rescorer : rescorers) {
      if (rescorer.hasQueryRescorer()) {
        QueryRescorer queryRescorer = rescorer.getQueryRescorer();
        Query query = queryNodeMapper.getQuery(queryRescorer.getRescoreQuery(), indexState);
        QueryRescore queryRescore =
            new QueryRescore(
                query, queryRescorer.getQueryWeight(), queryRescorer.getRescoreQueryWeight());
        hits = queryRescore.rescore(searcher, hits, queryRescorer.getWindowSize());
      }
      if (rescorer.hasScriptRescorer()) {
        ScriptRescorer scriptRescorer = rescorer.getScriptRescorer();
        ScoreScript.Factory scriptFactory =
            ScriptService.getInstance().compile(scriptRescorer.getScript(), ScoreScript.CONTEXT);
        Map<String, Object> params =
            ScriptParamsUtils.decodeParams(scriptRescorer.getScript().getParamsMap());
        DoubleValuesSource doubleValuesSource =
            scriptFactory.newFactory(params, indexState.docLookup);

        ScriptRescore scriptRescore = new ScriptRescore(doubleValuesSource);
        hits = scriptRescore.rescore(searcher, hits, scriptRescorer.getWindowSize());
      }
    }

    return hits;
  }
}
