package org.apache.platypus.server.luceneserver;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.platypus.server.grpc.QueryType;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

/**
 * This class maps our GRPC Query object to a Lucene Query object.
 */
class QueryNodeMapper {

    private final Map<org.apache.platypus.server.grpc.BooleanClause.Occur, BooleanClause.Occur> occurMapping
            = initializeOccurMapping();

    Query getQuery(org.apache.platypus.server.grpc.Query query) {
        QueryType queryType = query.getQueryType();
        switch (queryType) {
            case BOOLEAN_QUERY: return getBooleanQuery(query.getBooleanQuery());
            case PHRASE_QUERY: return getPhraseQuery(query.getPhraseQuery());
            default: throw new UnsupportedOperationException("Unsupported query type received: " + queryType);
        }
    }

    private BooleanQuery getBooleanQuery(org.apache.platypus.server.grpc.BooleanQuery booleanQuery) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder()
                .setMinimumNumberShouldMatch(booleanQuery.getMinimumNumberShouldMatch());

        booleanQuery.getClausesList()
                .forEach(clause -> builder
                        .add(getQuery(clause.getQuery()), occurMapping.get(clause.getOccur()))
                );

        return builder.build();
    }

    private PhraseQuery getPhraseQuery(org.apache.platypus.server.grpc.PhraseQuery phraseQuery) {
        PhraseQuery.Builder builder = new PhraseQuery.Builder()
                .setSlop(phraseQuery.getSlop());

        phraseQuery.getTermsList()
                .forEach(term -> builder.add(new Term(phraseQuery.getField(), term)));

        return builder.build();
    }

    private Map<org.apache.platypus.server.grpc.BooleanClause.Occur, BooleanClause.Occur> initializeOccurMapping() {
        return Arrays.stream(org.apache.platypus.server.grpc.BooleanClause.Occur.values())
                .filter(v -> v != org.apache.platypus.server.grpc.BooleanClause.Occur.UNRECOGNIZED)
                .collect(
                        () -> new EnumMap<>(org.apache.platypus.server.grpc.BooleanClause.Occur.class),
                        (map, v) -> map.put(v, BooleanClause.Occur.valueOf(v.name())),
                        EnumMap::putAll
                );
    }

}
