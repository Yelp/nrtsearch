package org.apache.platypus.server;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
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
import io.grpc.stub.StreamObserver;
import org.apache.platypus.server.config.LuceneServerConfiguration;
import org.apache.platypus.server.grpc.*;
import org.apache.platypus.server.luceneserver.GlobalState;
import org.apache.platypus.server.utils.OneDocBuilder;
import org.apache.platypus.server.utils.ParallelDocumentIndexer;
import org.junit.rules.TemporaryFolder;
import org.locationtech.spatial4j.io.GeohashUtils;

import static org.apache.platypus.server.YelpReviewsTest.startIndex;

public class YelpSuggestTest {


//    TemporaryFolder folder = new TemporaryFolder();
//    String nodeName = "serverSuggest";
//    String rootDirName = "testSuggestjava";
//    Path rootDir = folder.newFolder(rootDirName).toPath();
//    String testIndex = "test_index";
//    private LuceneServerConfiguration luceneServerConfiguration;
//    private final LuceneServerGrpc.LuceneServerBlockingStub blockingStub = new LuceneServerGrpc.LuceneServerBlockingStub();
//
//
//
//    GlobalState globalState = new GlobalState(nodeName, rootDir, null, 14700, 14700);
//
//
//    private LuceneServer.LuceneServerImpl myserver = new LuceneServer.LuceneServerImpl(globalState);
//
//
//    public yelpSuggest(LuceneServerConfiguration luceneServerConfiguration) throws IOException {
//        this.luceneServerConfiguration = luceneServerConfiguration;
//    }

//    private void createIndex() throws IOException{
//        // create req and stream and call
//        //  createIndex(CreateIndexRequest req, StreamObserver<CreateIndexResponse> responseObserver)
//
////        StreamObserver<CreateIndexResponse> responseObserver = new StreamObserver<CreateIndexResponse>();
////
////        CreateIndexRequest createIndexRequest = CreateIndexRequest.newBuilder().setIndexName(testIndex).setRootDir(rootDirName).build();
////        this.myserver.createIndex(createIndexRequest, );
//
//    }


    public static final String INDEX_NAME = "yelp_suggest_test_3";
    public static final String SUGGEST_NAME_DEFAULT = "suggest_default";

    private static void liveSettings(LuceneServerClient serverClient) {
        LiveSettingsRequest liveSettingsRequest = LiveSettingsRequest.newBuilder()
                .setIndexName(INDEX_NAME)
                .setIndexRamBufferSizeMB(256.0)
                .setMaxRefreshSec(1.0)
                .build();
        LiveSettingsResponse liveSettingsResponse = serverClient.getBlockingStub().liveSettings(liveSettingsRequest);
//        logger.info(liveSettingsResponse.getResponse());
    }

    private static void registerFields(LuceneServerClient serverClient) throws IOException {
//        String registerFieldsJson = Files.readString(Paths.get("/Users/fzz/platypus/src/test/resources/registerFieldsYelpSuggestTestPayload.json"));
        String registerFieldsJson = Files.readString(Paths.get("/Users/fzz/platypus/src/test/resources/registerFieldsYelpSuggestTest.json"));

        FieldDefRequest fieldDefRequest = YelpReviewsTest.getFieldDefRequest(registerFieldsJson);
        FieldDefResponse fieldDefResponse = serverClient.getBlockingStub().registerFields(fieldDefRequest);
//        logger.info(fieldDefResponse.getResponse());
    }

    private static void createIndex(LuceneServerClient serverClient, Path dir) {
        CreateIndexResponse response = serverClient.getBlockingStub().createIndex(
                CreateIndexRequest.newBuilder()
                        .setIndexName(INDEX_NAME)
                        .setRootDir(dir.resolve("index").toString())
                        .build());
//        logger.info(response.getResponse());
    }


    private static void setUpIndex(LuceneServerClient standaloneServerClient, Path standaloneDir) throws IOException, ExecutionException, InterruptedException {
        // create index if not exist
        try {
            createIndex(standaloneServerClient, standaloneDir);
        } catch(StatusRuntimeException e){
            if (!e.getStatus().getCode().name().equals("ALREADY_EXISTS"))
                throw e;
        }
        //live settings
        liveSettings(standaloneServerClient);
        // INFO: LiveSettingsHandler returned response: "{\"maxRefreshSec\":1.0,\"indexRamBufferSizeMB\":256.0}"
        //register fields aka add mapping , catch `java.lang.IllegalArgumentException: field "localized_completed_text" was already registered`
        registerFields(standaloneServerClient);
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

//        Path suggestionsPath = Paths.get("/Users/fzz/platypus/src/test/resources/business_index_results_prod_10012020.json");
        Path suggestionsPath = Paths.get("/Users/fzz/platypus/src/test/resources/business_index_results.json");
//        Path suggestionsPath = Paths.get("/Users/fzz/platypus/src/test/resources/business_index_results_2500.json");

        List<Future<Long>> results = ParallelDocumentIndexer.buildAndIndexDocs(
                new OneDocBuilderImpl(),
                suggestionsPath,
                indexService,
                standaloneServerClient
        );

        //wait till all indexing done and notify search thread once done
        for (Future<Long> each : results) {
            try {
                Long genId = each.get();
//            logger.info(String.format("ParallelDocumentIndexer.buildAndIndexDocs returned genId: %s", genId));

            }
            catch (ExecutionException | InterruptedException futureException){
                System.out.println(futureException.getCause());
            }
        }
        long t2 = System.nanoTime();

        System.out.println(
                String.format("it took %s to index dox", (t2 - t1))
        );
        // commit
        standaloneServerClient.getBlockingStub().commit(CommitRequest.newBuilder().setIndexName(INDEX_NAME).build());

    }

    public static void main(String[] args) throws IOException, Exception{
        // The dir where the indices will live in the remote server
        Path yelp_suggest_test_base_path = Paths.get("/nail/tmp");
        // The client who will be talking to the remote server
        LuceneServerClient standaloneServerClient = new LuceneServerClient("dev83-uswest1adevc", 55886);
        Path standaloneDir = yelp_suggest_test_base_path.resolve("standalone_3");


//        setUpIndex(standaloneServerClient, standaloneDir);



        // TODO add exception handling



//        int counter = 0;
//        String oneLine =  reader.readLine();
//        int linesread = 1;
//        while(oneLine != null){
//            linesread ++;
//            builder.add(OneDocBuilderImpl.buildOneDoc(oneLine));
//            counter ++;
//            if (counter == 25000 || one_biz == null){
//                System.out.println(String.format("Counter is %s LinesRead is %s", counter, linesread));
//                counter = 0;
//                Stream<AddDocumentRequest> addDocumentRequestStream = builder.build();
//                try {
//                    Long genId = new YelpReviewsTest.IndexerTask().index(standaloneServerClient, addDocumentRequestStream);
//                }
//                catch (Exception e){
//                    e.printStackTrace();
//                }
//                        builder = Stream.builder();
////                addDocumentRequestStream.close();
//            }
//        }



        // build Suggester
//        BuildSuggestRequest.Builder buildSuggestRequestBuilder = BuildSuggestRequest.newBuilder();
//        buildSuggestRequestBuilder.setSuggestName("suggest_0");
//        buildSuggestRequestBuilder.setIndexName(INDEX_NAME);
//        buildSuggestRequestBuilder.setInfixSuggester(InfixSuggester.newBuilder().setAnalyzer("default").build());
//        buildSuggestRequestBuilder.setNonLocalSource(SuggestNonLocalSource.newBuilder()
//                .setSuggestField("localized_completed_text")
//                .setWeightField("score")
//                .setContextField("geo_context")
//                .build());
//
//        BuildSuggestResponse response = standaloneServerClient.getBlockingStub().buildSuggest(
//                buildSuggestRequestBuilder.build()
//        );



//        System.out.println(response.getCount());

        SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
        suggestLookupBuilder.setIndexName(INDEX_NAME);
        suggestLookupBuilder.setText("a");
        suggestLookupBuilder.setSuggestName("suggest_0");
        suggestLookupBuilder.setHighlight(true);

        //Set SF lat lon lookup
//        List<String> sanFranGeohashes = getGeoHashes(37.7749, -122.4194, 5, 7);
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
                addField("category_aliases", one_biz.getCategory_aliases().toString(), addDocumentRequestBuilder);
                addField("checkin_rate_per_day",
                        one_biz.getCheckin_rate_per_day().toString(),
                        addDocumentRequestBuilder);
                addField("country", one_biz.getCountry(), addDocumentRequestBuilder);
                addField("id",
                        one_biz.getId().toString(), addDocumentRequestBuilder);
                addField("language_alternate_names", one_biz.getLanguage_alternate_names().toString(), addDocumentRequestBuilder);
                addField("localized_completed_text", one_biz.getLocalized_completed_text(), addDocumentRequestBuilder);
                addField("location", List.of(
                        one_biz.getLocationLat().toString(),
                        one_biz.getLocationLon().toString()
                ), addDocumentRequestBuilder);
                addField("review_count", one_biz.getReview_count().toString(), addDocumentRequestBuilder);
                addField("review_wilson_score",
                        one_biz.getReview_wilson_score().toString(),
                        addDocumentRequestBuilder);
                addField("score",
                        one_biz.getScore().toString(),
                        addDocumentRequestBuilder);
                addField("standardized_score",
                        one_biz.getStandardized_score().toString(),
                        addDocumentRequestBuilder);
                addField("unique_id",
                        one_biz.getUnique_id().toString(),
                        addDocumentRequestBuilder);
                addField("geo_context",
                        getGeoHashes(
                                one_biz.getLocationLat(),
                                one_biz.getLocationLon(),
                                5, 7),
                        addDocumentRequestBuilder);
            }
            catch (Exception hey){
                System.out.println(hey.getLocalizedMessage());
            }
            AddDocumentRequest addDocumentRequest = addDocumentRequestBuilder.build();
            return  addDocumentRequest;
        }

    }
}
