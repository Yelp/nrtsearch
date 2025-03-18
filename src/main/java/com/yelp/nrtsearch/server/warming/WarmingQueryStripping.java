package com.yelp.nrtsearch.server.warming;

import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.VirtualField;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.stream.Collectors;

public class WarmingQueryStripping {
    private final int maxRescorerStrippingPerc;
    private final int maxFunctionScoreScriptStrippingPerc;
    private final int maxVirtualFieldsStrippingPerc;
    private final int maxFacetsStrippingPerc;
    private final Random random;

    private int strippedRescorerCount;
    private int strippedFunctionScoreScriptCount;
    private int strippedVirtualFieldsCount;
    private int strippedFacetsCount;

    private int strippedQueryCount;

    public WarmingQueryStripping(
            int maxRescorerStrippingPerc,
            int maxFunctionScoreScriptStrippingPerc,
            int maxVirtualFieldsStrippingPerc,
            int maxFacetsStrippingPerc) {
        this.maxRescorerStrippingPerc = maxRescorerStrippingPerc;
        this.maxFunctionScoreScriptStrippingPerc = maxFunctionScoreScriptStrippingPerc;
        this.maxVirtualFieldsStrippingPerc = maxVirtualFieldsStrippingPerc;
        this.maxFacetsStrippingPerc = maxFacetsStrippingPerc;

        this.random = new Random();
        this.strippedRescorerCount = 0;
        this.strippedFunctionScoreScriptCount = 0;
        this.strippedVirtualFieldsCount = 0;
        this.strippedFacetsCount = 0;
    }

    public SearchRequest stripWarmingQuery(SearchRequest.Builder builder, int warmingCount) {
        int probability = random.nextInt(99);

        boolean stripRescorer = shouldStripWarmingQuery(strippedRescorerCount, probability, maxRescorerStrippingPerc, warmingCount);
        if (stripRescorer) {
            stripRescorers(builder);
        }

        boolean stripFunctionScoreScript = shouldStripWarmingQuery(strippedFunctionScoreScriptCount, probability, maxFunctionScoreScriptStrippingPerc, warmingCount);
        if (stripFunctionScoreScript) {
            stripFunctionScoreScript(builder);
        }

        boolean stripVirtualFields = shouldStripWarmingQuery(strippedVirtualFieldsCount, probability, maxVirtualFieldsStrippingPerc, warmingCount);
        if (stripVirtualFields) {
            stripVirtualFields(builder);
        }

        boolean stripFacets = shouldStripWarmingQuery(strippedFacetsCount, probability, maxFacetsStrippingPerc, warmingCount);
        if (stripFacets) {
            stripFacets(builder);
        }

        if (stripRescorer || stripFunctionScoreScript || stripVirtualFields || stripFacets) {
            strippedQueryCount++;
        }

        return builder.build();
    }

    public int getStrippedQueryCount() {
        return strippedQueryCount;
    }

    public String getStrippedQueryPercDetails(int warmingQueriesCount) {
        String warmingQueryStrippingDetails = String.format("Rescorers: %d, FunctionScoreScript: %d, VirtualFields: %d, Facets: %d",
                strippedRescorerCount / warmingQueriesCount * 100,
                strippedFunctionScoreScriptCount / warmingQueriesCount * 100,
                strippedVirtualFieldsCount / warmingQueriesCount * 100,
                strippedFacetsCount / warmingQueriesCount * 100);
        return (getStrippedQueryCount() > 0) ? warmingQueryStrippingDetails : "No queries were stripped.";
    }

    private void stripRescorers(SearchRequest.Builder builder) {
        builder.clearRescorers();
        // also need to skip fetch tasks as they can depend on rescorers
        builder.clearFetchTasks();
        strippedRescorerCount++;
    }

    private void stripFunctionScoreScript(SearchRequest.Builder builder) {
        if (builder.hasQuery() && builder.getQuery().hasFunctionScoreQuery()) {
            Query query = builder.getQuery();
            FunctionScoreQuery functionScoreQuery = query.getFunctionScoreQuery();
            builder.setQuery(functionScoreQuery.getQuery());
            strippedFunctionScoreScriptCount++;
        }
    }

    private void stripVirtualFields(SearchRequest.Builder builder) {
        if (builder.getVirtualFieldsCount() > 0) {
            ArrayList<String> retrieveFieldsWithoutVirtualField = new ArrayList<>();

            // skipping fields from retrieve fields that were created in virtual fields and facets
            // before clearing virtual fields and facets
            HashSet<String> virtualFieldNames = builder.getVirtualFieldsList().stream()
                    .map(VirtualField::getName).collect(Collectors.toCollection(HashSet::new));

            for (int i = 0; i < builder.getRetrieveFieldsCount(); i++) {
                String retrieveFieldName = builder.getRetrieveFields(i);
                if (!virtualFieldNames.contains(retrieveFieldName)) {
                    retrieveFieldsWithoutVirtualField.add(retrieveFieldName);
                }
            }
            // also skipping virtual fields and facets as they can depend on virtual fields
            builder.clearRetrieveFields();
            builder.addAllRetrieveFields(retrieveFieldsWithoutVirtualField);

            builder.clearVirtualFields();
            builder.clearFacets();
            strippedVirtualFieldsCount++;
        }
    }

    private void stripFacets(SearchRequest.Builder builder) {
        builder.clearFacets();
        strippedFacetsCount++;
    }

    private boolean shouldStripWarmingQuery(int strippedQueryCount,  int probability, int maxStrippedWarmingQueriesPerc, int warmingQueriesCount) {
        // make sure we don't strip more than max so far in case too many probabilities are low enough. For example:
        // 2 warming queries, with max stripping perc at 80% and both checks the probability is 70,
        // so we would end up stripping 100% of queries.
        return probability < maxStrippedWarmingQueriesPerc && strippedQueryCount + 1 <= warmingQueriesCount * maxStrippedWarmingQueriesPerc / 100;
    }
}
