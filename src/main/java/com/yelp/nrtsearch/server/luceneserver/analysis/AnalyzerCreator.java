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
package com.yelp.nrtsearch.server.luceneserver.analysis;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.ConditionalTokenFilter;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.NameAndParams;
import com.yelp.nrtsearch.server.plugins.AnalysisPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

public class AnalyzerCreator {

  private static final String LUCENE_ANALYZER_PATH = "org.apache.lucene.analysis.{0}Analyzer";
  private static final String STANDARD = "standard";
  private static final String CLASSIC = "classic";

  private static AnalyzerCreator instance;

  private final Map<String, AnalysisProvider<? extends Analyzer>> analyzerMap = new HashMap<>();

  public AnalyzerCreator(LuceneServerConfiguration configuration) {
    register(STANDARD, name -> new StandardAnalyzer());
    register(CLASSIC, name -> new ClassicAnalyzer());
  }

  public Analyzer getAnalyzer(com.yelp.nrtsearch.server.grpc.Analyzer analyzer) {
    if (!analyzer.getPredefined().isEmpty()) {
      String predefinedAnalyzer = analyzer.getPredefined();

      if (analyzerMap.containsKey(predefinedAnalyzer)) {
        return analyzerMap.get(predefinedAnalyzer).get(predefinedAnalyzer);
      } else {
        // Try to dynamically load the analyzer class
        try {
          String className = MessageFormat.format(LUCENE_ANALYZER_PATH, predefinedAnalyzer);
          return (Analyzer)
              AnalyzerCreator.class
                  .getClassLoader()
                  .loadClass(className)
                  .getDeclaredConstructor()
                  .newInstance();
        } catch (InstantiationException
            | IllegalAccessException
            | NoSuchMethodException
            | ClassNotFoundException
            | InvocationTargetException e) {
          throw new AnalyzerCreationException(
              "Unable to find predefined analyzer: " + predefinedAnalyzer, e);
        }
      }
    } else if (analyzer.hasCustom()) {
      return getCustomAnalyzer(analyzer.getCustom());
    } else {
      throw new AnalyzerCreationException("Unable to find or create analyzer: " + analyzer);
    }
  }

  private void register(Map<String, AnalysisProvider<? extends Analyzer>> analyzers) {
    analyzers.forEach(this::register);
  }

  private void register(String name, AnalysisProvider<? extends Analyzer> provider) {
    if (analyzerMap.containsKey(name)) {
      throw new IllegalArgumentException("Analyzer " + name + " already exists");
    }
    analyzerMap.put(name, provider);
  }

  public static void initialize(LuceneServerConfiguration configuration, Iterable<Plugin> plugins) {
    instance = new AnalyzerCreator(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof AnalysisPlugin) {
        AnalysisPlugin analysisPlugin = (AnalysisPlugin) plugin;
        instance.register(analysisPlugin.getAnalyzers());
      }
    }
  }

  public static AnalyzerCreator getInstance() {
    return instance;
  }

  /**
   * Create an {@link Analyzer} from user parameters. Note that we create new maps with the param
   * maps because the Protobuf one may be unmodifiable and Lucene may modify the maps.
   */
  private static Analyzer getCustomAnalyzer(
      com.yelp.nrtsearch.server.grpc.CustomAnalyzer analyzer) {
    CustomAnalyzer.Builder builder = CustomAnalyzer.builder();

    if (analyzer.hasPositionIncrementGap()) {
      builder.withPositionIncrementGap(analyzer.getPositionIncrementGap().getInt());
    }
    if (analyzer.hasOffsetGap()) {
      builder.withOffsetGap(analyzer.getOffsetGap().getInt());
    }

    try {
      if (!analyzer.getDefaultMatchVersion().isEmpty()) {
        builder.withDefaultMatchVersion(Version.parseLeniently(analyzer.getDefaultMatchVersion()));
      }

      for (NameAndParams charFilter : analyzer.getCharFiltersList()) {
        builder.addCharFilter(charFilter.getName(), new HashMap<>(charFilter.getParamsMap()));
      }

      builder.withTokenizer(
          analyzer.getTokenizer().getName(), new HashMap<>(analyzer.getTokenizer().getParamsMap()));

      for (NameAndParams tokenFilter : analyzer.getTokenFiltersList()) {
        builder.addTokenFilter(tokenFilter.getName(), new HashMap<>(tokenFilter.getParamsMap()));
      }

      // TODO: The only impl of ConditionalTokenFilter is ProtectedTermFilter
      // (https://lucene.apache.org/core/8_2_0/analyzers-common/org/apache/lucene/analysis/miscellaneous/ProtectedTermFilterFactory.html)
      // It needs a protected terms file as input which is not supported yet.
      for (ConditionalTokenFilter conditionalTokenFilter :
          analyzer.getConditionalTokenFiltersList()) {
        NameAndParams condition = conditionalTokenFilter.getCondition();
        CustomAnalyzer.ConditionBuilder when =
            builder.when(condition.getName(), condition.getParamsMap());

        for (NameAndParams tokenFilter : conditionalTokenFilter.getTokenFiltersList()) {
          when.addTokenFilter(tokenFilter.getName(), tokenFilter.getParamsMap());
        }

        when.endwhen();
      }

      return builder.build();
    } catch (ParseException | IOException e) {
      throw new AnalyzerCreationException("Unable to create custom analyzer: " + analyzer, e);
    }
  }

  // TODO: replace usages of this method in suggest with getAnalyzer
  public static Analyzer getStandardAnalyzer() {
    return new StandardAnalyzer();
  }

  public static boolean hasAnalyzer(Field field) {
    return field != null
        && (isAnalyzerDefined(field.getAnalyzer())
            || isAnalyzerDefined(field.getIndexAnalyzer())
            || isAnalyzerDefined(field.getSearchAnalyzer()));
  }

  public static boolean isAnalyzerDefined(com.yelp.nrtsearch.server.grpc.Analyzer analyzer) {
    return analyzer != null && (!analyzer.getPredefined().isEmpty() || analyzer.hasCustom());
  }

  static class AnalyzerCreationException extends RuntimeException {

    AnalyzerCreationException(String message) {
      super(message);
    }

    AnalyzerCreationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
