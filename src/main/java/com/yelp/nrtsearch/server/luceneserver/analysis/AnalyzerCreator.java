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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.Version;

public class AnalyzerCreator {

  private static final String LUCENE_ANALYZER_PATH = "org.apache.lucene.analysis.{0}Analyzer";
  private static final String STANDARD = "standard";
  private static final String CLASSIC = "classic";

  private static AnalyzerCreator instance;

  private final LuceneServerConfiguration configuration;
  private final Map<String, AnalysisProvider<? extends Analyzer>> analyzerMap = new HashMap<>();
  private final Map<String, Class<? extends TokenFilterFactory>> tokenFilterMap = new HashMap<>();
  private final Map<String, Class<? extends CharFilterFactory>> charFilterMap = new HashMap<>();

  public AnalyzerCreator(LuceneServerConfiguration configuration) {
    this.configuration = configuration;
    registerAnalyzer(STANDARD, name -> new StandardAnalyzer());
    registerAnalyzer(CLASSIC, name -> new ClassicAnalyzer());
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

  private void registerAnalyzers(Map<String, AnalysisProvider<? extends Analyzer>> analyzers) {
    analyzers.forEach(this::registerAnalyzer);
  }

  private void registerAnalyzer(String name, AnalysisProvider<? extends Analyzer> provider) {
    if (analyzerMap.containsKey(name)) {
      throw new IllegalArgumentException("Analyzer " + name + " already exists");
    }
    analyzerMap.put(name, provider);
  }

  private void registerTokenFilters(
      Map<String, Class<? extends TokenFilterFactory>> tokenFilters, Set<String> builtIn) {
    tokenFilters.forEach((k, v) -> registerTokenFilter(k, v, builtIn));
  }

  private void registerTokenFilter(
      String name, Class<? extends TokenFilterFactory> filterClass, Set<String> builtIn) {
    if (builtIn.contains(name.toLowerCase())) {
      throw new IllegalArgumentException("Token filter " + name + " already provided by lucene");
    }
    if (tokenFilterMap.containsKey(name)) {
      throw new IllegalArgumentException("Token filter " + name + " already exists");
    }
    tokenFilterMap.put(name, filterClass);
  }

  private void registerCharFilters(
      Map<String, Class<? extends CharFilterFactory>> charFilters, Set<String> builtIn) {
    charFilters.forEach((k, v) -> registerCharFilter(k, v, builtIn));
  }

  private void registerCharFilter(
      String name, Class<? extends CharFilterFactory> filterClass, Set<String> builtIn) {
    if (builtIn.contains(name.toLowerCase())) {
      throw new IllegalArgumentException("Char filter " + name + " already provided by lucene");
    }
    if (charFilterMap.containsKey(name)) {
      throw new IllegalArgumentException("Char filter " + name + " already exists");
    }
    charFilterMap.put(name, filterClass);
  }

  public static void initialize(LuceneServerConfiguration configuration, Iterable<Plugin> plugins) {
    instance = new AnalyzerCreator(configuration);
    Set<String> builtInTokenFilters =
        TokenFilterFactory.availableTokenFilters().stream()
            .map(String::toLowerCase)
            .collect(Collectors.toSet());
    Set<String> builtInCharFilters =
        CharFilterFactory.availableCharFilters().stream()
            .map(String::toLowerCase)
            .collect(Collectors.toSet());
    instance.registerCharFilter(
        MappingV2CharFilterFactory.NAME, MappingV2CharFilterFactory.class, builtInCharFilters);
    for (Plugin plugin : plugins) {
      if (plugin instanceof AnalysisPlugin) {
        AnalysisPlugin analysisPlugin = (AnalysisPlugin) plugin;
        instance.registerAnalyzers(analysisPlugin.getAnalyzers());
        instance.registerTokenFilters(analysisPlugin.getTokenFilters(), builtInTokenFilters);
        instance.registerCharFilters(analysisPlugin.getCharFilters(), builtInCharFilters);
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
  private Analyzer getCustomAnalyzer(com.yelp.nrtsearch.server.grpc.CustomAnalyzer analyzer) {
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
        // check first to see if this filter was registered
        Class<? extends CharFilterFactory> filterClass = charFilterMap.get(charFilter.getName());
        if (filterClass != null) {
          builder.addCharFilter(filterClass, new HashMap<>(charFilter.getParamsMap()));
        } else {
          builder.addCharFilter(charFilter.getName(), new HashMap<>(charFilter.getParamsMap()));
        }
      }

      builder.withTokenizer(
          analyzer.getTokenizer().getName(), new HashMap<>(analyzer.getTokenizer().getParamsMap()));

      for (NameAndParams tokenFilter : analyzer.getTokenFiltersList()) {
        // check first to see if this filter was registered by a plugin
        Class<? extends TokenFilterFactory> filterClass = tokenFilterMap.get(tokenFilter.getName());
        if (filterClass != null) {
          builder.addTokenFilter(filterClass, new HashMap<>(tokenFilter.getParamsMap()));
        } else {
          builder.addTokenFilter(tokenFilter.getName(), new HashMap<>(tokenFilter.getParamsMap()));
        }
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

      return initializeComponents(builder.build());
    } catch (ParseException | IOException e) {
      throw new AnalyzerCreationException("Unable to create custom analyzer: " + analyzer, e);
    }
  }

  /**
   * Components registered by plugins may depend on information from nrtsearch, like config file
   * parameters. Components that implement the {@link AnalysisComponent} interface have this
   * information injected here, after the {@link CustomAnalyzer} is built.
   */
  private CustomAnalyzer initializeComponents(CustomAnalyzer customAnalyzer) {
    for (TokenFilterFactory filterFactory : customAnalyzer.getTokenFilterFactories()) {
      if (filterFactory instanceof AnalysisComponent) {
        ((AnalysisComponent) filterFactory).initializeComponent(configuration);
      }
    }
    for (CharFilterFactory filterFactory : customAnalyzer.getCharFilterFactories()) {
      if (filterFactory instanceof AnalysisComponent) {
        ((AnalysisComponent) filterFactory).initializeComponent(configuration);
      }
    }
    return customAnalyzer;
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
