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
package com.yelp.nrtsearch.server;

import com.google.gson.Gson;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.BuildSuggestRequest;
import com.yelp.nrtsearch.server.grpc.BuildSuggestResponse;
import com.yelp.nrtsearch.server.grpc.InfixSuggester;
import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import com.yelp.nrtsearch.server.grpc.OneSuggestLookupResponse;
import com.yelp.nrtsearch.server.grpc.SuggestLookupRequest;
import com.yelp.nrtsearch.server.grpc.SuggestLookupResponse;
import com.yelp.nrtsearch.server.grpc.SuggestNonLocalSource;
import com.yelp.nrtsearch.server.utils.OneDocBuilder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.locationtech.spatial4j.io.GeohashUtils;

public class YelpSuggestTest extends TestIndexManager {
  private static final String SUGGESTIONS_FILE_PATH =
      Paths.get("src", "test", "resources", "yelp_5_business_suggestions.json")
          .toAbsolutePath()
          .toString();

  public static final String INDEX_NAME = "yelp_suggest_test_3";

  private static BuildSuggestResponse buildSuggester(LuceneServerClient standaloneServerClient) {
    BuildSuggestRequest.Builder buildSuggestRequestBuilder = BuildSuggestRequest.newBuilder();
    buildSuggestRequestBuilder.setSuggestName("suggest_0");
    buildSuggestRequestBuilder.setIndexName(INDEX_NAME);
    buildSuggestRequestBuilder.setInfixSuggester(
        InfixSuggester.newBuilder().setAnalyzer("default").build());
    buildSuggestRequestBuilder.setNonLocalSource(
        SuggestNonLocalSource.newBuilder()
            .setSuggestField("localized_completed_text")
            .setWeightField("score")
            .setContextField("geo_context")
            .setPayloadField("payload")
            .build());

    return standaloneServerClient
        .getBlockingStub()
        .buildSuggest(buildSuggestRequestBuilder.build());
  }

  // Run with args host && port && remote tmp dir in order
  public static void main(String[] args) throws Exception {
    // The dir where the indices will live in the remote server
    Path yelp_suggest_test_base_path = Paths.get(System.getProperty("suggestTmp"));
    // The client who will be talking to the remote server
    LuceneServerClient standaloneServerClient =
        new LuceneServerClient(
            System.getProperty("suggestHost"), Integer.parseInt(System.getProperty("suggestPort")));
    Path standaloneDir = yelp_suggest_test_base_path.resolve("standalone_3");

    setUpIndex(
        standaloneServerClient,
        standaloneDir,
        INDEX_NAME,
        SUGGESTIONS_FILE_PATH,
        new YelpSuggestTest.OneDocBuilderImpl());

    // build Suggester
    buildSuggester(standaloneServerClient);

    // look up suggestions
    SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
    suggestLookupBuilder.setIndexName(INDEX_NAME);
    suggestLookupBuilder.setText("a");
    suggestLookupBuilder.setSuggestName("suggest_0");
    suggestLookupBuilder.setHighlight(true);

    // Set SF lat lon lookup
    List<String> sanFranGeohashes = getGeoHashes(37.785371, -122.459446, 5, 7);

    for (String geohash : sanFranGeohashes) {
      suggestLookupBuilder.addContexts(geohash);
    }

    SuggestLookupResponse suggestResponse =
        standaloneServerClient.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
    List<OneSuggestLookupResponse> suggestResponseResultsList = suggestResponse.getResultsList();

    System.out.println(suggestResponseResultsList);

    System.exit(0);
  }

  private static List<String> getGeoHashes(
      double latitude, double longitude, int minPrecision, int maxPrecision) {
    List<String> geohashes = new ArrayList<>();
    for (int i = minPrecision; i <= maxPrecision; i++) {
      geohashes.add(GeohashUtils.encodeLatLon(latitude, longitude, i));
    }
    return geohashes;
  }

  private static class OneDocBuilderImpl implements OneDocBuilder {

    @Override
    public AddDocumentRequest buildOneDoc(String line, Gson gson) {
      AddDocumentRequest.Builder addDocumentRequestBuilder = AddDocumentRequest.newBuilder();
      addDocumentRequestBuilder.setIndexName(INDEX_NAME);
      BusinessSuggestionRecord one_biz = gson.fromJson(line, BusinessSuggestionRecord.class);
      try {

        addField("score", one_biz.getScore().toString(), addDocumentRequestBuilder);
        addField("payload", one_biz.toString(), addDocumentRequestBuilder);
        addField(
            "geo_context",
            getGeoHashes(one_biz.getLocationLat(), one_biz.getLocationLon(), 5, 7),
            addDocumentRequestBuilder);
        addField(
            "localized_completed_text",
            one_biz.getLocalizedCompletedText(),
            addDocumentRequestBuilder);
      } catch (Exception hey) {
        System.out.println(hey.getLocalizedMessage());
      }
      AddDocumentRequest addDocumentRequest = addDocumentRequestBuilder.build();
      return addDocumentRequest;
    }
  }

  private static class BusinessSuggestionRecord {

    private Long unique_id;
    private Long score;
    private Long id;
    private String localized_completed_text;
    private Map<String, Double> location;
    private String country;
    private Integer review_count;
    private Double review_wilson_score;
    private List<String> category_aliases;
    private Double checkin_rate_per_day;
    private Long standardized_score;
    private List<Map<String, String>> language_alternate_names;

    public Long getUniqueId() {
      return unique_id;
    }

    public Long getScore() {
      return score;
    }

    public Long getId() {
      return id;
    }

    public String getLocalizedCompletedText() {
      return localized_completed_text;
    }

    public Map<String, Double> getLocation() {
      return location;
    }

    public Double getLocationLat() {
      return getLocation().get("y");
    }

    public Double getLocationLon() {
      return getLocation().get("x");
    }

    @Override
    public String toString() {
      return "BusinessSuggestionRecord{"
          + "unique_id="
          + unique_id
          + ", score="
          + score
          + ", id="
          + id
          + ", localized_completed_text='"
          + localized_completed_text
          + '\''
          + ", location="
          + location
          + ", country='"
          + country
          + '\''
          + ", review_count="
          + review_count
          + ", review_wilson_score="
          + review_wilson_score
          + ", category_aliases="
          + category_aliases
          + ", checkin_rate_per_day="
          + checkin_rate_per_day
          + ", standardized_score="
          + standardized_score
          + ", language_alternate_names="
          + language_alternate_names
          + '}';
    }

    public String getCountry() {
      return country;
    }

    public Integer getReviewCount() {
      return review_count;
    }

    public Double getReviewWilsonScore() {
      return review_wilson_score;
    }

    public List<String> getCategoryAliases() {
      return category_aliases;
    }

    public Double getCheckinRatePerDay() {
      return checkin_rate_per_day;
    }

    public Long getStandardizedScore() {
      return standardized_score;
    }

    public List<Map<String, String>> getLanguageAlternateNames() {
      return language_alternate_names;
    }
  }

  @Test
  public void runYelpSuggest() throws Exception {
    YelpSuggestTest.main(null);
  }
}
