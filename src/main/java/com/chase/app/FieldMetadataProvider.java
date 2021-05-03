package com.chase.app;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import com.chase.app.analyzers.HighlightingSearchAnalyzer;
import com.chase.app.analyzers.PathLiteDelimitionAnalyzer;
import com.chase.app.analyzers.TextAggressiveDelimitionAnalyzer;
import com.chase.app.analyzers.TextPrefixSearchAnalyzer;
import com.chase.app.analyzers.TypeAggressiveDelimitionAnalyzer;
import com.chase.app.search.FieldMetadata;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;

public class FieldMetadataProvider {
    final static Map<String, FieldMetadata> _map = Arrays.stream(new FieldMetadata[] {
        new FieldMetadata(IndexedFieldsNames.ID),
        new FieldMetadata(IndexedFieldsNames.APP_ID, (Analyzer)null, new WhitespaceAnalyzer()),   // TODO hack for split-on-whitespace. Missing normalization
        new FieldMetadata(IndexedFieldsNames.UPDATE_ID),
        new FieldMetadata(IndexedFieldsNames.ACCOUNT_ID),
        new FieldMetadata(IndexedFieldsNames.LINK_ID),
        new FieldMetadata(IndexedFieldsNames.TYPE), // TODO normalizer for case insensitiveness? need to add
        new FieldMetadata(IndexedFieldsNames.TYPE, IndexedFieldsExtensionsNames.LITE_DELIMITION,
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.TYPE, IndexedFieldsExtensionsNames.LITE_DELIMITION)),
        new FieldMetadata(IndexedFieldsNames.TYPE, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION,
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.TYPE, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION)),
        new FieldMetadata(IndexedFieldsNames.TYPE, IndexedFieldsExtensionsNames.PREFIXES,
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.TYPE, IndexedFieldsExtensionsNames.PREFIXES),
            new TypeAggressiveDelimitionAnalyzer()),
        
        new FieldMetadata(IndexedFieldsNames.EXTERNAL_ID),
        new FieldMetadata(IndexedFieldsNames.FETCH_INPUT),
        
        new FieldMetadata(IndexedFieldsNames.NAME, FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.NAME)), 
        new FieldMetadata(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.LITE_DELIMITION, 
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.LITE_DELIMITION)), 
        new FieldMetadata(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION, 
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION)),
         new FieldMetadata(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX, 
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX),
            new TextAggressiveDelimitionAnalyzer()),
        new FieldMetadata(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.SHINGLES, 
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.SHINGLES)),
        new FieldMetadata(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.STEMMING, 
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.STEMMING)),
        new FieldMetadata(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.PREFIXES, 
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.PREFIXES),
            new TextPrefixSearchAnalyzer()),
        new FieldMetadata(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.KEYWORD, 
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.NAME, IndexedFieldsExtensionsNames.KEYWORD)),

            new FieldMetadata(IndexedFieldsNames.TEXT, FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.TEXT)), 
            new FieldMetadata(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.LITE_DELIMITION, 
                FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.LITE_DELIMITION)), 
            new FieldMetadata(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION, 
                FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION)),
             new FieldMetadata(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX, 
                FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX),
                new TextAggressiveDelimitionAnalyzer()),
            new FieldMetadata(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.SHINGLES, 
                FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.SHINGLES)),
            new FieldMetadata(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.STEMMING, 
                FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.STEMMING)),
            new FieldMetadata(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.PREFIXES, 
                FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.TEXT, IndexedFieldsExtensionsNames.PREFIXES),
                new TextPrefixSearchAnalyzer()),
        new FieldMetadata(IndexedFieldsNames.HIGHLIGHT, FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.HIGHLIGHT), new HighlightingSearchAnalyzer()),

        new FieldMetadata(IndexedFieldsNames.CONTENT, FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.CONTENT)),
        new FieldMetadata(IndexedFieldsNames.CONTENT, IndexedFieldsExtensionsNames.STEMMING, 
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.CONTENT, IndexedFieldsExtensionsNames.STEMMING)),

        new FieldMetadata(IndexedFieldsNames.PATHS, FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.PATHS)), // TODO
        new FieldMetadata(IndexedFieldsNames.PATHS, IndexedFieldsExtensionsNames.LITE_DELIMITION, 
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.PATHS, IndexedFieldsExtensionsNames.LITE_DELIMITION)),
        new FieldMetadata(IndexedFieldsNames.PATHS, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION, 
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.PATHS, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION)),
        new FieldMetadata(IndexedFieldsNames.PATHS, IndexedFieldsExtensionsNames.LITE_DELIMITION_PREFIX, 
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.PATHS, IndexedFieldsExtensionsNames.LITE_DELIMITION_PREFIX),
            new PathLiteDelimitionAnalyzer()),
        new FieldMetadata(IndexedFieldsNames.PATHS, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX, 
            FieldIndexAnalyzerProvider.GetIndexAnalyzer(IndexedFieldsNames.PATHS, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX),
            new TextAggressiveDelimitionAnalyzer()),
        

        new FieldMetadata(IndexedFieldsNames.INDEX_TIME)
        
        // TODO : traits and data?
    }).collect(Collectors.toMap(x -> x.FullName(), x -> x));

    public static FieldMetadata Get(String key) {
        if (_map.containsKey(key))
            return _map.get(key);
        
        throw new RuntimeException("Unknown field");
    }
}