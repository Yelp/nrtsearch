package com.yelp.nrtsearch.server.luceneserver.warming;

import com.yelp.nrtsearch.server.grpc.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.stream.Collectors;

public class WarmingQueryStripping {
    private final int maxRescorerStrippingPerc;
    private final int maxScriptQueriesStrippingPerc;
    private final int maxVirtualFieldsStrippingPerc;
    private final int maxFacetsStrippingPerc;
    private final Random random;

    private int strippedRescorerCount;
    private int strippedScriptQueriesCount;
    private int strippedVirtualFieldsCount;
    private int strippedFacetsCount;

    private int strippedQueryCount;

    public WarmingQueryStripping(
            int maxRescorerStrippingPerc,
            int maxScriptQueriesStrippingPerc,
            int maxVirtualFieldsStrippingPerc,
            int maxFacetsStrippingPerc) {
        this.maxRescorerStrippingPerc = maxRescorerStrippingPerc;
        this.maxScriptQueriesStrippingPerc = maxScriptQueriesStrippingPerc;
        this.maxVirtualFieldsStrippingPerc = maxVirtualFieldsStrippingPerc;
        this.maxFacetsStrippingPerc = maxFacetsStrippingPerc;

        this.random = new Random();
        this.strippedRescorerCount = 0;
        this.strippedScriptQueriesCount = 0;
        this.strippedVirtualFieldsCount = 0;
        this.strippedFacetsCount = 0;
    }

    public SearchRequest stripWarmingQuery(SearchRequest.Builder builder, int warmingCount) {
        boolean stripRescorer = shouldStripWarmingQuery(strippedRescorerCount, maxRescorerStrippingPerc, warmingCount);
        if (stripRescorer) {
            stripRescorers(builder);
        }

        boolean stripScriptQueries = shouldStripWarmingQuery(strippedScriptQueriesCount, maxScriptQueriesStrippingPerc, warmingCount);
        if (stripScriptQueries) {
            stripScriptQueries(builder);
        }

        boolean stripVirtualFields = shouldStripWarmingQuery(strippedVirtualFieldsCount, maxVirtualFieldsStrippingPerc, warmingCount);
        boolean stripFacetsOnly = false;
        if (stripVirtualFields) {
            stripVirtualFields(builder);
        } else { // because virtual fields already strips facets
            stripFacetsOnly = shouldStripWarmingQuery(strippedFacetsCount, maxFacetsStrippingPerc, warmingCount);
            if (stripFacetsOnly) {
                stripFacets(builder);
            }
        }

        if (stripRescorer || stripScriptQueries || stripVirtualFields || stripFacetsOnly) {
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
                strippedScriptQueriesCount / warmingQueriesCount * 100,
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

    private void stripScriptQueries(SearchRequest.Builder builder) {
        builder.setQuery(stripScriptQuery(builder.getQuery()));
        strippedScriptQueriesCount++;
    }

    private static Query stripScriptQuery(Query query) {
        if (query.hasFunctionScoreQuery()) {
            return stripScriptQuery(query.getFunctionScoreQuery().getQuery());
        }
        if (query.hasFunctionFilterQuery()) {
            return Query.newBuilder().build();
        }
        if (query.hasMultiFunctionScoreQuery()) {
            MultiFunctionScoreQuery multiFunctionScoreQuery = query.getMultiFunctionScoreQuery();
            for (MultiFunctionScoreQuery.FilterFunction function : multiFunctionScoreQuery.getFunctionsList()) {
                if (function.hasScript()) {
                    return stripScriptQuery(function.getFilter());
                }
            }
        }

        Query.Builder queryBuilder = query.toBuilder();
        switch (query.getQueryNodeCase()) {
            case BOOLEANQUERY:
                queryBuilder.setBooleanQuery(stripBooleanQuery(query.getBooleanQuery()));
                break;
            case DISJUNCTIONMAXQUERY:
                queryBuilder.setDisjunctionMaxQuery(stripDisjunctionMaxQuery(query.getDisjunctionMaxQuery()));
                break;
            case NESTEDQUERY:
                queryBuilder.setNestedQuery(stripNestedQuery(query.getNestedQuery()));
                break;
            case CONSTANTSCOREQUERY:
                queryBuilder.setConstantScoreQuery(stripConstantScoreQuery(query.getConstantScoreQuery()));
                break;
            // Add other cases as needed
            default:
                break;
        }
        return queryBuilder.build();
    }

    private static BooleanQuery stripBooleanQuery(BooleanQuery booleanQuery) {
        BooleanQuery.Builder booleanQueryBuilder = booleanQuery.toBuilder();
        for (int i = 0; i < booleanQuery.getClausesCount(); i++) {
            BooleanClause clause = booleanQuery.getClauses(i);
            BooleanClause.Builder clauseBuilder = clause.toBuilder();
            clauseBuilder.setQuery(stripScriptQuery(clause.getQuery()));
            booleanQueryBuilder.setClauses(i, clauseBuilder.build());
        }
        return booleanQueryBuilder.build();
    }

    private static DisjunctionMaxQuery stripDisjunctionMaxQuery(DisjunctionMaxQuery disjunctionMaxQuery) {
        DisjunctionMaxQuery.Builder disjunctionMaxQueryBuilder = disjunctionMaxQuery.toBuilder();
        for (int i = 0; i < disjunctionMaxQuery.getDisjunctsCount(); i++) {
            disjunctionMaxQueryBuilder.setDisjuncts(i, stripScriptQuery(disjunctionMaxQuery.getDisjuncts(i)));
        }
        return disjunctionMaxQueryBuilder.build();
    }

    private static NestedQuery stripNestedQuery(NestedQuery nestedQuery) {
        NestedQuery.Builder nestedQueryBuilder = nestedQuery.toBuilder();
        nestedQueryBuilder.setQuery(stripScriptQuery(nestedQuery.getQuery()));
        return nestedQueryBuilder.build();
    }

    private static ConstantScoreQuery stripConstantScoreQuery(ConstantScoreQuery constantScoreQuery) {
        ConstantScoreQuery.Builder constantScoreQueryBuilder = constantScoreQuery.toBuilder();
        constantScoreQueryBuilder.setFilter(stripScriptQuery(constantScoreQuery.getFilter()));
        return constantScoreQueryBuilder.build();
    }

    private boolean shouldStripWarmingQuery(int strippedQueryCount, int maxStrippedWarmingQueriesPerc, int warmingQueriesCount) {
        int probability = random.nextInt(99);
        // make sure we don't strip more than max so far in case too many probabilities are low enough. For example:
        // 2 warming queries, with max stripping perc at 80% and both checks the probability is 70,
        // so we would end up stripping 100% of queries.
        return probability < maxStrippedWarmingQueriesPerc && strippedQueryCount + 1 <= warmingQueriesCount * maxStrippedWarmingQueriesPerc / 100;
    }
}
