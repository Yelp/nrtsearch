package org.apache.platypus.server;


import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import com.google.gson.Gson;
import io.grpc.StatusRuntimeException;
import org.apache.platypus.server.grpc.*;
import org.apache.platypus.server.utils.OneDocBuilder;
import org.apache.platypus.server.utils.ParallelDocumentIndexer;
import org.locationtech.spatial4j.io.GeohashUtils;


public class YelpSuggestTest extends YelpBaseTest {
    private static final String SUGGESTIONS_FILE_PATH =
            Paths.get("src", "test", "resources", "yelp_5_business_suggestions.json").toAbsolutePath().toString();


    public static final String INDEX_NAME = "yelp_suggest_test_3";

    private static void setUpIndex(LuceneServerClient standaloneServerClient, Path standaloneDir) throws IOException, ExecutionException, InterruptedException {
        // create index if it does not exist
        try {
            createIndex(standaloneServerClient, standaloneDir, INDEX_NAME);
        } catch(StatusRuntimeException e){
            if (!e.getStatus().getCode().name().equals("ALREADY_EXISTS"))
                throw e;
        }
        //add live settings
        liveSettings(standaloneServerClient, INDEX_NAME);

        // register fields
        registerFields(standaloneServerClient,  Paths.get("src", "test", "resources", "registerFieldsYelpSuggestTestPayload.json").toAbsolutePath().toString());

        // start index
        StartIndexRequest startIndexRequest = StartIndexRequest.newBuilder()
                .setIndexName(INDEX_NAME)
                .setMode(Mode.STANDALONE)
                .setPrimaryGen(0)
                .setRestore(false)
                .build();
        startIndex(standaloneServerClient, startIndexRequest);
        // index docs
        long t1 = System.nanoTime();
        Stream.Builder<AddDocumentRequest> builder = Stream.builder();

        final ExecutorService indexService = YelpReviewsTest.createExecutorService(
                (Runtime.getRuntime().availableProcessors()) / 4,
                "LuceneIndexing");

        Path suggestionsPath = Paths.get(SUGGESTIONS_FILE_PATH);


        List<Future<Long>> results = ParallelDocumentIndexer.buildAndIndexDocs(
                new OneDocBuilderImpl(),
                suggestionsPath,
                indexService,
                standaloneServerClient
        );

        //wait till all indexing done
        for (Future<Long> each : results) {
            try {
                Long genId = each.get();
                logger.info(String.format("ParallelDocumentIndexer.buildAndIndexDocs returned genId: %s", genId));
            }
            catch (ExecutionException | InterruptedException futureException){
                System.out.println(futureException.getCause());
            }
        }
        long t2 = System.nanoTime();

        System.out.println(
                String.format("IT took %s nanosecs to index documents", (t2 - t1))
        );

        // commit
        standaloneServerClient.getBlockingStub().commit(CommitRequest.newBuilder().setIndexName(INDEX_NAME).build());

    }

    private static BuildSuggestResponse buildSuggester(LuceneServerClient standaloneServerClient){
        BuildSuggestRequest.Builder buildSuggestRequestBuilder = BuildSuggestRequest.newBuilder();
        buildSuggestRequestBuilder.setSuggestName("suggest_0");
        buildSuggestRequestBuilder.setIndexName(INDEX_NAME);
        buildSuggestRequestBuilder.setInfixSuggester(InfixSuggester.newBuilder().setAnalyzer("default").build());
        buildSuggestRequestBuilder.setNonLocalSource(SuggestNonLocalSource.newBuilder()
                .setSuggestField("localized_completed_text")
                .setWeightField("score")
                .setContextField("geo_context")
                .setPayloadField("payload")
                .build());

        return standaloneServerClient.getBlockingStub().buildSuggest(
                buildSuggestRequestBuilder.build()
        );
    }

    public static void main(String[] args) throws IOException, Exception{
        // The dir where the indices will live in the remote server
        Path yelp_suggest_test_base_path = Paths.get("/nail/tmp");
        // The client who will be talking to the remote server
        LuceneServerClient standaloneServerClient = new LuceneServerClient("dev83-uswest1adevc", 55886);
        Path standaloneDir = yelp_suggest_test_base_path.resolve("standalone_3");

        setUpIndex(standaloneServerClient, standaloneDir);

        // build Suggester
        buildSuggester(standaloneServerClient);

        // look up suggestions
        SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
        suggestLookupBuilder.setIndexName(INDEX_NAME);
        suggestLookupBuilder.setText("a");
        suggestLookupBuilder.setSuggestName("suggest_0");
        suggestLookupBuilder.setHighlight(true);

        //Set SF lat lon lookup
        List<String> sanFranGeohashes = getGeoHashes(37.785371, -122.459446, 5, 7);

        for (String geohash : sanFranGeohashes) {
            suggestLookupBuilder.addContexts(geohash);
        }

        SuggestLookupResponse suggestResponse = standaloneServerClient.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
        List<OneSuggestLookupResponse> suggestResponseResultsList = suggestResponse.getResultsList();

        System.out.println(suggestResponseResultsList);

        System.exit(0);

    }

    private static List<String> getGeoHashes(double latitude, double longitude, int minPrecision, int maxPrecision) {
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

                addField("score",
                        one_biz.getScore().toString(),
                        addDocumentRequestBuilder);
                addField("payload",
                        one_biz.toString(),
                        addDocumentRequestBuilder);
                addField("geo_context",
                        getGeoHashes(
                                one_biz.getLocationLat(),
                                one_biz.getLocationLon(),
                                5, 7),
                        addDocumentRequestBuilder);
                addField("localized_completed_text", one_biz.getLocalized_completed_text(), addDocumentRequestBuilder);
            }
            catch (Exception hey){
                System.out.println(hey.getLocalizedMessage());
            }
            AddDocumentRequest addDocumentRequest = addDocumentRequestBuilder.build();
            return  addDocumentRequest;
        }

    }

    public static class BusinessSuggestionRecord {

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

        public Long getUnique_id() {
            return unique_id;
        }

        public Long getScore() {
            return score;
        }

        public Long getId() {
            return id;
        }

        public String getLocalized_completed_text() {
            return localized_completed_text;
        }

        public Map<String, Double> getLocation() {
            return location;
        }

        public Double getLocationLat(){
            return getLocation().get("y");
        }

        public Double getLocationLon(){
            return getLocation().get("x");
        }

        @Override
        public String toString() {
            return "BusinessSuggestionRecord{" +
                    "unique_id=" + unique_id +
                    ", score=" + score +
                    ", id=" + id +
                    ", localized_completed_text='" + localized_completed_text + '\'' +
                    ", location=" + location +
                    ", country='" + country + '\'' +
                    ", review_count=" + review_count +
                    ", review_wilson_score=" + review_wilson_score +
                    ", category_aliases=" + category_aliases +
                    ", checkin_rate_per_day=" + checkin_rate_per_day +
                    ", standardized_score=" + standardized_score +
                    ", language_alternate_names=" + language_alternate_names +
                    '}';
        }

        public String getCountry() {
            return country;
        }

        public Integer getReview_count() {
            return review_count;
        }

        public Double getReview_wilson_score() {
            return review_wilson_score;
        }

        public List<String> getCategory_aliases() {
            return category_aliases;
        }

        public Double getCheckin_rate_per_day() {
            return checkin_rate_per_day;
        }

        public Long getStandardized_score() {
            return standardized_score;
        }

        public List<Map<String, String>> getLanguage_alternate_names() {
            return language_alternate_names;
        }
    }
}
