package com.chase.app.search;

import java.io.Console;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.chase.app.FieldMetadataProvider;
import com.chase.app.IndexedFieldsExtensionsNames;
import com.chase.app.IndexedFieldsNames;
import com.chase.app.search.contracts.SearchFilter;
import com.google.common.collect.Multimap;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.builders.DisjunctionMaxQueryBuilder;
import org.apache.lucene.search.BlendedTermQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
// import org.elasticsearch.cluster.routing.allocation.decider.Decision.Multi;
// import org.elasticsearch.index.query.TermsQueryBuilder;

public class SearcherQueryBuilder {

    private TokenFactory _analyzer;

    public SearcherQueryBuilder() throws IOException    
    {
        this._analyzer = new TokenFactory();
    }


    
    public Query ApplyPrefilter(Query query,  SearchFilter filter) {
        BooleanQuery.Builder _builder = new BooleanQuery.Builder();

        _builder.add(query, Occur.MUST);

        if (filter.appId != null) {
            _builder.add( TermsInSet(Field(IndexedFieldsNames.APP_ID).Field, filter.appId), Occur.FILTER);
        }

        if (filter.resourceType != null) {
            _builder.add( TermsInSet(Field(IndexedFieldsNames.TYPE).Field, filter.resourceType), Occur.FILTER);
        }

        if (filter.link != null) {
            _builder.add( TermsInSet(Field(IndexedFieldsNames.LINK_ID).Field, filter.link), Occur.FILTER);
        }

        return _builder.build();
    }

    public Query BuildInnerQuery(String query) { // TODO move to a separate class
        
        BooleanQuery.Builder _builder = new BooleanQuery.Builder();
        
        _builder.add(BuildFilter(query), Occur.FILTER);
        _builder.add(BuildPositiveQuery(query), Occur.MUST); // TODO there should be a "boosting" query here, think how to simulate with Lucene. We'll only use the positive for now.
        return _builder.build();
    }
    
    public Query BuildFilter(String query)
    {
        BooleanQuery.Builder _builder = new BooleanQuery.Builder();
        _builder.add(Term("chase:sitelink", Field(IndexedFieldsNames.TYPE).Field), Occur.MUST_NOT);

        String[] filterWords = Arrays.stream(query.split("[\\s_\\-\\\\\\/]+")).filter(s -> !STOP_WORDS.contains(s)).limit(20).toArray(String[]::new);
        int c = filterWords.length;
        
        _builder.setMinimumNumberShouldMatch(c > 2 ? c - 1 : c);
        
        Arrays.stream(filterWords).forEach(s -> {
            _builder.add(MultiMatch(s, (Float)null, null, 
                Field(IndexedFieldsNames.APP_ID),
                Field(IndexedFieldsNames.TYPE),
                Field(IndexedFieldsNames.TYPE, IndexedFieldsExtensionsNames.LITE_DELIMITION),
                Field(IndexedFieldsNames.TYPE, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION),
                Field(IndexedFieldsNames.TYPE, IndexedFieldsExtensionsNames.PREFIXES),

                Field(IndexedFieldsNames.NAME),
                Field(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.LITE_DELIMITION),
                Field(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION),
                Field(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.SHINGLES),
                Field(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.STEMMING),
                Field(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.PREFIXES),

                Field(IndexedFieldsNames.TEXT),
                Field(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.LITE_DELIMITION),
                Field(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION),
                Field(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.SHINGLES),
                Field(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.STEMMING),
                Field(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.PREFIXES),

                Field(IndexedFieldsNames.PATHS, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION),
                Field(IndexedFieldsNames.PATHS, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX)
            ) , Occur.SHOULD);
        });

        return _builder.build();
    }
    
    Set<String> STOP_WORDS = Arrays.stream(
        "a, an, and, are, as, at, be, but, by, for, if, in, into, is, it, no, not, of, on, or, such, that, the, their, then, there, these, they, this, to, was, will, with".split(", "))
        .collect(Collectors.toSet());
    
    public Query BuildPositiveQuery(String query)
    {
        // TODO : in ES we had a content-based dismax query here. However we don't enable content search at this point so currently ignored.
        return BuildResourcesQuery(query);   
    }

    public FieldRequest Field(String key, Float boost) {
        

        FieldRequest req = new FieldRequest();
        req.Boost=boost;
        req.Field = FieldMetadataProvider.Get(key);

        return req;
    }

    public FieldRequest Field(String key)
    {
        return Field(key, (Float)null);
    }

    public FieldRequest Field(String key, String extension, Float boost) {
        return Field(String.format("%s.%s", key, extension));
    }
    public FieldRequest Field(String key, String extension) {
        return Field(key, extension, null);
    }

    public Query BuildResourcesQuery(String query) {
        BooleanQuery.Builder _builder = new BooleanQuery.Builder();

        _builder.add(MultiMatch(query, 0.2F, 10.0F, Field("appId"), Field("type")), Occur.SHOULD);

        _builder.add(MultiMatch(query, MultiMatchType.MostFields, 9F, 
            Field(IndexedFieldsNames.NAME), 
            Field(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.LITE_DELIMITION, 0.4F),
            Field(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION, 0.3F),
            Field(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.SHINGLES, 0.6F),
            Field(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.STEMMING, 0.7F)
            ), Occur.SHOULD);
        
        _builder.add(MultiMatch(query, MultiMatchType.MostFields, 4F, 
            Field(IndexedFieldsNames.TEXT), 
            Field(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.LITE_DELIMITION, 0.4F),
            Field(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION, 0.3F),
            Field(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.SHINGLES, 0.6F),
            Field(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.STEMMING, 0.7F)
            ), Occur.SHOULD);
        
        _builder.add(MatchPhrase(query, Field(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.PREFIXES).Field, 6F, 1), Occur.SHOULD);
        _builder.add(MatchPhrase(query, Field(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.PREFIXES).Field, 4F, 1), Occur.SHOULD);
        _builder.add(Match(query, Field(IndexedFieldsNames.PATHS, IndexedFieldsExtensionsNames.LITE_DELIMITION_PREFIX).Field, MatchOperator.And, 0.6F), Occur.SHOULD);
        _builder.add(Match(query, Field(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX).Field, MatchOperator.And, 1F), Occur.SHOULD);

        _builder.add(MultiMatch(query, MultiMatchType.MostFields, 9F,  
            Field(IndexedFieldsNames.TYPE, IndexedFieldsExtensionsNames.LITE_DELIMITION, 1F),
            Field(IndexedFieldsNames.TYPE, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION, 0.6F),
            Field(IndexedFieldsNames.TYPE, IndexedFieldsExtensionsNames.PREFIXES, 0.1F)
            ), Occur.SHOULD);
        
        // TODO support fuzziness - idea would be to change the "term" implementation to support fuzziness - hence replacing "TermQuery" with "FuzzyQuery". The rest will simply bubble the fuzzy request down.


        return _builder.build();
    }


    public Query AllowBoost(Query q, Float boost)
    {
        if (boost == null || boost == 1)
            return q; 
        else return new BoostQuery(q, boost);
    }

    public Query Term(String query, FieldMetadata meta) {
       return Term(query, meta, null);
    }

    public Query Term(String query, FieldMetadata meta, Float boost) {
        Query q = new TermQuery(new Term(meta.FullName(), query)); /// TODO - does lucene analyze the text? otherwise why use the field name?
        return AllowBoost(q, boost);
    }

    public Query Synonym(FieldMetadata meta, Float boost, String... queries)
    {
        org.apache.lucene.search.SynonymQuery.Builder builder = new SynonymQuery.Builder(meta.FullName());
        Arrays.stream(queries).map(s -> new Term(meta.FullName(), s)).forEach(t -> builder.addTerm(t));
        Query q = builder.build();
        return AllowBoost(q, boost);
    }

    public enum MatchOperator
    {
        Or,
        And
    }

    public enum MultiMatchType
    {
        MostFields,
        BestField,
        CrossFields,

    }


    public Query MatchPhrase(String query, FieldMetadata meta, Float boost, Integer slop)
    {
        if (!meta.Analyzed) {
            return Term(query, meta, boost);
        }
        
        PhraseQuery.Builder _builder = new PhraseQuery.Builder();
        if (slop != null) {
            _builder.setSlop(slop);
        }
        try {
            ArrayList<TokenData> tokens = _analyzer.AnalyzeIntoTokens(meta, query);
            tokens.stream().sorted(Comparator.comparingInt(TokenData::GetPosition))
                .forEach(t -> _builder.add(new Term(meta.FullName(), t.Text), t.Position));

            return _builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Query TermsInSet(FieldMetadata field, String[] terms) {
        return new TermInSetQuery(
            field.FullName(), 
            Arrays.stream(terms).map(s -> new BytesRef(s)).toArray(BytesRef[]::new));
    }

    public Query Match(
        String query, 
        FieldMetadata meta, 
        MatchOperator operator,
        Float boost)
      //  throws IOException 
        {
                                        // TODO: elasticsearch return the sum of all matches for all tokens. Does Lucene do the same the way we used in bool query?
        if (!meta.Analyzed)
        {
            return Term(query, meta, boost);
        }

        
        try {
            ArrayList<TokenData> tokens = _analyzer.AnalyzeIntoTokens(meta, query);
            if (tokens.size() == 1) {
                return Term(tokens.get(0).Text, meta, boost);
            }
            BooleanQuery.Builder _builder = new BooleanQuery.Builder();
            tokens.stream()
                .collect(Collectors.groupingBy(d -> d.Position))
                .entrySet().stream()
                .map(s -> {
                    if (s.getValue().size() == 1) { 
                        return Term(s.getValue().get(0).Text, meta, boost); 
                    }
                    return Synonym(meta, boost, s.getValue().stream().map(x -> x.Text).toArray(String[]::new));
                }).forEach(q -> _builder.add(q, operator == MatchOperator.Or ? Occur.SHOULD : Occur.MUST));
            return _builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public class FieldRequest {
        public FieldMetadata Field;
        public Float Boost = null;
    }

    private Query BestFieldMultiMatch(String query, Float tieBreaker, MatchOperator op, FieldRequest... fields)
    {
        List<Query> disjuncts = Arrays.asList(
            Arrays.stream(fields).map(f -> this.Match(query, f.Field, op, f.Boost)).toArray(Query[]::new));
        return new DisjunctionMaxQuery(disjuncts, tieBreaker == null ? 0 : tieBreaker);
    }

    private Query MostFieldsMultiMatch(String query, Float tieBreaker, MatchOperator op, Integer minimumShouldMatch, FieldRequest... fields)
    {
        BooleanQuery.Builder _builder = new BooleanQuery.Builder();
        if (minimumShouldMatch != null) {
            _builder.setMinimumNumberShouldMatch(minimumShouldMatch);
        }
        Arrays.stream(fields).map(f -> this.Match(query, f.Field, op, f.Boost)).forEach(q -> _builder.add(q, Occur.SHOULD));
        return _builder.build();
    }

    

    public Query MultiMatch(String query, MultiMatchType type, Float tieBreaker, MatchOperator op, Float boost, Integer minimumShouldMatch, FieldRequest... fields) // TODO  boost,fuzzy,
    {
        switch (type) {
            case BestField:
                return AllowBoost(BestFieldMultiMatch(query, tieBreaker, op, fields), boost);
            case MostFields: 
                return AllowBoost(MostFieldsMultiMatch(query, tieBreaker, op, minimumShouldMatch, fields), boost);
        default:
            throw new RuntimeException("Unsupported type");
        }
    }

    public Query MultiMatch(String query, MultiMatchType type, Float boost, FieldRequest... fields)
    {
        return MultiMatch(query, type, null, MatchOperator.Or, boost, null, fields);
    }

    public Query MultiMatch(String query, Float tieBreaker, Float boost, FieldRequest... fields)
    {
        return MultiMatch(query, MultiMatchType.BestField, tieBreaker, MatchOperator.Or, boost, null, fields);
    }
}