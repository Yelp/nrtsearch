package com.chase.app.plugins;

import com.chase.app.analyzers.CustomStamAnalyzer;
import com.chase.app.analyzers.CustomTokensAnalyzer;
import com.chase.app.analyzers.HighlightingPrefixAnalyzer;
import com.chase.app.analyzers.HighlightingSearchAnalyzer;
import com.chase.app.analyzers.PathLiteDelimitionAnalyzer;
import com.chase.app.analyzers.PathLiteDelimitionPrefixAnalyzer;
import com.chase.app.analyzers.TextAggressiveDelimitionAnalyzer;
import com.chase.app.analyzers.TextAggressiveDelimitionPrefixAnalyzer;
import com.chase.app.analyzers.TextBasicAnalyzer;
import com.chase.app.analyzers.TextLiteDelimitionAnalyzer;
import com.chase.app.analyzers.TextPrefixAnalyzer;
import com.chase.app.analyzers.TextPrefixSearchAnalyzer;
import com.chase.app.analyzers.TextShingleAnalyzer;
import com.chase.app.analyzers.TextStemmingAnalyzer;
import com.chase.app.analyzers.TypeAggressiveDelimitionAnalyzer;
import com.chase.app.analyzers.TypeLiteDelimitionAnalyzer;
import com.chase.app.analyzers.TypePrefixAnalyzer;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.analysis.AnalysisProvider;
import com.yelp.nrtsearch.server.plugins.AnalysisPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import java.util.Map;
import java.io.IOException;
import java.util.HashMap;

public class ChaseAnalyzersPlugin extends Plugin implements AnalysisPlugin {

  public ChaseAnalyzersPlugin(LuceneServerConfiguration config) {

  }

  public Map<String, AnalysisProvider<? extends Analyzer>> getAnalyzers() {
    Map<String, AnalysisProvider<? extends Analyzer>> map = new HashMap<String, AnalysisProvider<? extends Analyzer>>();
    map.put("customeStam", new AnalysisProvider<CustomStamAnalyzer>() {
      public CustomStamAnalyzer get(String name) {
        return new CustomStamAnalyzer();
      }
    });
    map.put("customTokens", new AnalysisProvider<CustomTokensAnalyzer>() {
      public CustomTokensAnalyzer get(String name) {
        return new CustomTokensAnalyzer();
      }
    });
    map.put("highlightingPrefix", new AnalysisProvider<HighlightingPrefixAnalyzer>() {
      public HighlightingPrefixAnalyzer get(String name) {
        return new HighlightingPrefixAnalyzer();
      }
    });
    map.put("highlightingSearch", new AnalysisProvider<HighlightingSearchAnalyzer>() {
      public HighlightingSearchAnalyzer get(String name) {
        return new HighlightingSearchAnalyzer();
      }
    });
    map.put("pathLiteDelimition", new AnalysisProvider<PathLiteDelimitionAnalyzer>() {
      public PathLiteDelimitionAnalyzer get(String name) {
        return new PathLiteDelimitionAnalyzer();
      }
    });
    map.put("pathLiteDelimitionPrefix", new AnalysisProvider<PathLiteDelimitionPrefixAnalyzer>() {
      public PathLiteDelimitionPrefixAnalyzer get(String name) {
        return new PathLiteDelimitionPrefixAnalyzer();
      }
    });
    map.put("textAggressiveDelimition", new AnalysisProvider<TextAggressiveDelimitionAnalyzer>() {
      public TextAggressiveDelimitionAnalyzer get(String name) {
        return new TextAggressiveDelimitionAnalyzer();
      }
    });
    map.put("textAggressiveDelimitionPrefix", new AnalysisProvider<TextAggressiveDelimitionPrefixAnalyzer>() {
      public TextAggressiveDelimitionPrefixAnalyzer get(String name) {
        return new TextAggressiveDelimitionPrefixAnalyzer();
      }
    });
    map.put("textBasic", new AnalysisProvider<TextBasicAnalyzer>() {
      public TextBasicAnalyzer get(String name) {
        return new TextBasicAnalyzer();
      }
    });
    map.put("textLiteDelimition", new AnalysisProvider<TextLiteDelimitionAnalyzer>() {
      public TextLiteDelimitionAnalyzer get(String name) {
        return new TextLiteDelimitionAnalyzer();
      }
    });
    map.put("textPrefix", new AnalysisProvider<TextPrefixAnalyzer>() {
      public TextPrefixAnalyzer get(String name) {
        return new TextPrefixAnalyzer();
      }
    });
    map.put("textPrefixSearch", new AnalysisProvider<TextPrefixSearchAnalyzer>() {
      public TextPrefixSearchAnalyzer get(String name) {
        return new TextPrefixSearchAnalyzer();
      }
    });
    map.put("textShingle", new AnalysisProvider<TextShingleAnalyzer>() {
      public TextShingleAnalyzer get(String name) {
        return new TextShingleAnalyzer();
      }
    });
    map.put("textStemming", new AnalysisProvider<TextStemmingAnalyzer>() {
      public TextStemmingAnalyzer get(String name) {
        return new TextStemmingAnalyzer();
      }
    });
    map.put("treePathHierarchy", new AnalysisProvider<TreePathHierarchyAnalyzer>() {
      public TreePathHiepeAggressiveDelimitionAnalyzer get(String name) {
        return new TypeAggressiveDelimitionAnalyzer();
      }
    });
    map.put("typeLiteDelimition", new AnalysisProvider<TypeLiteDelimitionAnalyzer>() {
      public TypeLiteDelimitionAnalyzer get(String name) {
        return new TypeLiteDelimitionAnalyzer();
      }
    });
    map.put("typePrefix", new AnalysisProvider<TypePrefixAnalyzer>() {
      public TypePrefixAnalyzer get(String name) {
        return new TypePrefixAnalyzer();
      }
    });
    map.put("keyword", new AnalysisProvider<KeywordAnalyzer>() {
      public KeywordAnalyzer get(String name) {
        return new KeywordAnalyzer();
      }
    });
    return map;
  }
}
