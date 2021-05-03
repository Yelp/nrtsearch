package com.chase.app.search;

import java.io.IOException;
import java.io.NotActiveException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class TokenFactory { // TODO rename asap
    private LoadingCache<String, ArrayList<TokenData>> _cache;
    public TokenFactory() {
        CacheLoader<String, ArrayList<TokenData>> loader;
        loader = new CacheLoader<String, ArrayList<TokenData>>() {
            @Override
            public java.util.ArrayList<TokenData> load(String key) throws Exception {
                throw new NotActiveException("use put instead");
            };
        };

        _cache = CacheBuilder.newBuilder().expireAfterAccess(1500, TimeUnit.MILLISECONDS).build(loader);
    }


    public ArrayList<TokenData> AnalyzeIntoTokens(FieldMetadata field, String text) 
    throws IOException
    {
        String fieldName = field.Extension == null ? field.Name : String.format("%s.%s", field.Name, field.Extension);
        String cachekey = String.format("%s!$!%s", fieldName, text);

        ArrayList<TokenData> res = _cache.getIfPresent(cachekey);

        if (res == null) {
            res = AnalysisHelper.Analyze(fieldName, text, field.SearchAnalyzer == null ? field.Analyzer : field.SearchAnalyzer);
            _cache.put(cachekey, res);
        }

        return res;
    }
}