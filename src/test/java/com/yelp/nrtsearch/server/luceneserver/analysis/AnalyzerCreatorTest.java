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

import static com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator.getStandardAnalyzer;
import static com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator.hasAnalyzer;
import static com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator.isAnalyzerDefined;
import static org.apache.lucene.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.apache.lucene.util.LuceneTestCase.random;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.IntObject;
import com.yelp.nrtsearch.server.grpc.NameAndParams;
import com.yelp.nrtsearch.server.plugins.AnalysisPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.bn.BengaliAnalyzer;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.ClassicTokenizerFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(RandomizedRunner.class) // Required to call org.apache.lucene.util.LuceneTestCase.random
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class AnalyzerCreatorTest {

  @Before
  public void init() {
    init(Collections.emptyList());
  }

  private void init(List<Plugin> plugins) {
    AnalyzerCreator.initialize(getEmptyConfig(), plugins);
  }

  private LuceneServerConfiguration getEmptyConfig() {
    String config = "nodeName: \"lucene_server_foo\"";
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  // Tests for predefined analyzers

  @Test
  public void testPredefinedStandardAnalyzer() {
    Analyzer analyzer =
        AnalyzerCreator.getInstance().getAnalyzer(getPredefinedAnalyzer("standard"));

    assertSame(StandardAnalyzer.class, analyzer.getClass());
  }

  @Test
  public void testPredefinedClassicAnalyzer() {
    Analyzer analyzer = AnalyzerCreator.getInstance().getAnalyzer(getPredefinedAnalyzer("classic"));

    assertSame(ClassicAnalyzer.class, analyzer.getClass());
  }

  @Test
  public void testPredefinedDynamicallyInitializedAnalyzer() {
    List<String> names =
        Arrays.asList(
            "en.English", "bn.Bengali", "eu.Basque", "hy.Armenian", "ru.Russian", "th.Thai");
    List<Class> classes =
        Arrays.asList(
            EnglishAnalyzer.class,
            BengaliAnalyzer.class,
            BasqueAnalyzer.class,
            ArmenianAnalyzer.class,
            RussianAnalyzer.class,
            ThaiAnalyzer.class);

    assertEquals(names.size(), classes.size());

    for (int i = 0; i < names.size(); i++) {
      Analyzer analyzer =
          AnalyzerCreator.getInstance().getAnalyzer(getPredefinedAnalyzer(names.get(i)));
      assertSame(classes.get(i), analyzer.getClass());
    }
  }

  private static com.yelp.nrtsearch.server.grpc.Analyzer getPredefinedAnalyzer(String name) {
    return com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder().setPredefined(name).build();
  }

  // Tests for custom analyzers - created using tests in
  // org.apache.lucene.analysis.custom.TestCustomAnalyzer

  @Test
  public void testCustomAnalyzerFactoryHtmlStripClassicFolding() throws IOException {
    com.yelp.nrtsearch.server.grpc.Analyzer analyzerGrpc =
        com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder()
            .setCustom(
                com.yelp.nrtsearch.server.grpc.CustomAnalyzer.newBuilder()
                    .setDefaultMatchVersion("LATEST")
                    .addCharFilters(NameAndParams.newBuilder().setName("htmlstrip"))
                    .setTokenizer(NameAndParams.newBuilder().setName("classic"))
                    .addTokenFilters(
                        NameAndParams.newBuilder()
                            .setName("asciifolding")
                            .putParams("preserveOriginal", "true"))
                    .addTokenFilters(NameAndParams.newBuilder().setName("lowercase"))
                    .setPositionIncrementGap(IntObject.newBuilder().setInt(100))
                    .setOffsetGap(IntObject.newBuilder().setInt(1000)))
            .build();

    CustomAnalyzer analyzer =
        (CustomAnalyzer) AnalyzerCreator.getInstance().getAnalyzer(analyzerGrpc);
    assertHtmlStripClassicFolding(analyzer);
  }

  public static void assertHtmlStripClassicFolding(CustomAnalyzer analyzer) throws IOException {
    assertSame(ClassicTokenizerFactory.class, analyzer.getTokenizerFactory().getClass());
    List<CharFilterFactory> charFilters = analyzer.getCharFilterFactories();
    assertEquals(1, charFilters.size());
    assertEquals(HTMLStripCharFilterFactory.class, charFilters.get(0).getClass());
    List<TokenFilterFactory> tokenFilters = analyzer.getTokenFilterFactories();
    assertEquals(2, tokenFilters.size());
    assertSame(ASCIIFoldingFilterFactory.class, tokenFilters.get(0).getClass());
    assertSame(LowerCaseFilterFactory.class, tokenFilters.get(1).getClass());
    assertEquals(100, analyzer.getPositionIncrementGap("dummy"));
    assertEquals(1000, analyzer.getOffsetGap("dummy"));
    assertSame(Version.LATEST, analyzer.getVersion());

    assertAnalyzesTo(
        analyzer,
        "<p>foo bar</p> FOO BAR",
        new String[] {"foo", "bar", "foo", "bar"},
        new int[] {1, 1, 1, 1});
    assertAnalyzesTo(
        analyzer,
        "<p><b>föó</b> bär     FÖÖ BAR</p>",
        new String[] {"foo", "föó", "bar", "bär", "foo", "föö", "bar"},
        new int[] {1, 0, 1, 0, 1, 0, 1});
    analyzer.close();
  }

  public static void assertAnalyzesTo(
      Analyzer a, String input, String[] output, int[] posIncrements) throws IOException {
    assertAnalyzesTo(a, input, output, null, null, null, posIncrements, null);
  }

  public static void assertAnalyzesTo(
      Analyzer a,
      String input,
      String[] output,
      int startOffsets[],
      int endOffsets[],
      String types[],
      int posIncrements[],
      int posLengths[])
      throws IOException {
    checkResetException(a, input);
    BaseTokenStreamTestCase.checkAnalysisConsistency(random(), a, true, input);
    assertTokenStreamContents(
        a.tokenStream("dummy", input),
        output,
        startOffsets,
        endOffsets,
        types,
        posIncrements,
        posLengths,
        input.length());
  }

  private static void checkResetException(Analyzer a, String input) throws IOException {
    TokenStream ts = a.tokenStream("bogus", input);
    try {
      if (ts.incrementToken()) {
        fail("didn't get expected exception when reset() not called");
      }
    } catch (IllegalStateException expected) {
      // ok
    } catch (Exception unexpected) {
      unexpected.printStackTrace(System.err);
      fail("got wrong exception when reset() not called: " + unexpected);
    } finally {
      // consume correctly
      ts.reset();
      while (ts.incrementToken()) {}
      ts.end();
      ts.close();
    }

    // check for a missing close()
    ts = a.tokenStream("bogus", input);
    ts.reset();
    while (ts.incrementToken()) {}
    ts.end();
    try {
      ts = a.tokenStream("bogus", input);
      fail("didn't get expected exception when close() not called");
    } catch (IllegalStateException expected) {
      // ok
    } finally {
      ts.close();
    }
  }

  @Test
  public void testCustomAnalyzerNormalizationWithMultipleTokenFilters() {
    // none of these components are multi-term aware so they should not be applied
    com.yelp.nrtsearch.server.grpc.Analyzer analyzerGrpc =
        com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder()
            .setCustom(
                com.yelp.nrtsearch.server.grpc.CustomAnalyzer.newBuilder()
                    .setTokenizer(NameAndParams.newBuilder().setName("whitespace"))
                    .addTokenFilters(NameAndParams.newBuilder().setName("asciifolding"))
                    .addTokenFilters(NameAndParams.newBuilder().setName("lowercase")))
            .build();

    CustomAnalyzer analyzer =
        (CustomAnalyzer) AnalyzerCreator.getInstance().getAnalyzer(analyzerGrpc);

    assertEquals(new BytesRef("a b e"), analyzer.normalize("dummy", "À B é"));
  }

  @Test
  public void testCustomAnalyzerNormalizationWithMultipleCharFilters() {
    // none of these components are multi-term aware so they should not be applied
    com.yelp.nrtsearch.server.grpc.Analyzer analyzerGrpc =
        com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder()
            .setCustom(
                com.yelp.nrtsearch.server.grpc.CustomAnalyzer.newBuilder()
                    .addCharFilters(
                        NameAndParams.newBuilder()
                            .setName("mapping")
                            .putParams("mapping", "custom_analyzer_mapping/mapping1.txt"))
                    .addCharFilters(
                        NameAndParams.newBuilder()
                            .setName("mapping")
                            .putParams("mapping", "custom_analyzer_mapping/mapping2.txt"))
                    .setTokenizer(NameAndParams.newBuilder().setName("whitespace")))
            .build();

    CustomAnalyzer analyzer =
        (CustomAnalyzer) AnalyzerCreator.getInstance().getAnalyzer(analyzerGrpc);

    assertEquals(new BytesRef("e f c"), analyzer.normalize("dummy", "a b c"));
  }

  // Test for getStandardAnalyzer method

  @Test
  public void testGetStandardAnalyzer() {
    assertSame(StandardAnalyzer.class, getStandardAnalyzer().getClass());
  }

  // Tests for hasAnalyzer method

  @Test
  public void testHasAnalyzerNoField() {
    assertFalse(hasAnalyzer(null));
  }

  @Test
  public void testHasAnalyzerNoAnalyzer() {
    Field field = Field.newBuilder().build();
    assertFalse(hasAnalyzer(field));
  }

  @Test
  public void testHasAnalyzerAnalyzerPresent() {
    Field field = Field.newBuilder().setAnalyzer(getPredefinedAnalyzer()).build();

    assertTrue(hasAnalyzer(field));
  }

  @Test
  public void testHasAnalyzerIndexAnalyzerPresent() {
    Field field = Field.newBuilder().setIndexAnalyzer(getPredefinedAnalyzer()).build();

    assertTrue(hasAnalyzer(field));
  }

  @Test
  public void testHasAnalyzerSearchAnalyzerPresent() {
    Field field = Field.newBuilder().setIndexAnalyzer(getPredefinedAnalyzer()).build();

    assertTrue(hasAnalyzer(field));
  }

  private static com.yelp.nrtsearch.server.grpc.Analyzer getPredefinedAnalyzer() {
    return com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder().setPredefined("dummy").build();
  }

  // Tests for isAnalyzerDefined

  @Test
  public void testIsAnalyzerDefinedNoAnalyzer() {
    assertFalse(isAnalyzerDefined(null));
  }

  @Test
  public void testIsAnalyzerDefinedAnalyzerPresentPredefinedAndCustomAbsent() {
    boolean analyzerDefined =
        isAnalyzerDefined(com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder().build());
    assertFalse(analyzerDefined);
  }

  @Test
  public void testIsAnalyzerDefinedPredefinedPresentCustomAbsent() {
    boolean analyzerDefined = isAnalyzerDefined(getPredefinedAnalyzer());
    assertTrue(analyzerDefined);
  }

  @Test
  public void testIsAnalyzerDefinedCustomPresentPredefinedAbsent() {
    boolean analyzerDefined =
        isAnalyzerDefined(
            com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder()
                .setCustom(com.yelp.nrtsearch.server.grpc.CustomAnalyzer.newBuilder().build())
                .build());
    assertTrue(analyzerDefined);
  }

  @Test
  public void testIsAnalyzerDefinedPredefinedAndCustomPresent() {
    boolean analyzerDefined =
        isAnalyzerDefined(
            com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder()
                .setPredefined("dummy")
                .setCustom(com.yelp.nrtsearch.server.grpc.CustomAnalyzer.newBuilder().build())
                .build());
    assertTrue(analyzerDefined);
  }

  // test AnalysisPlugin

  static class TestAnalysisPlugin extends Plugin implements AnalysisPlugin {
    @Override
    public Map<String, AnalysisProvider<? extends Analyzer>> getAnalyzers() {
      Map<String, AnalysisProvider<? extends Analyzer>> analyzerMap = new HashMap<>();
      analyzerMap.put(
          "plugin_analyzer",
          (name) -> {
            try {
              return CustomAnalyzer.builder()
                  .withDefaultMatchVersion(Version.LATEST)
                  .addCharFilter("htmlstrip")
                  .withTokenizer("classic")
                  .addTokenFilter("asciifolding", "preserveOriginal", "true")
                  .addTokenFilter("lowercase")
                  .withPositionIncrementGap(100)
                  .withOffsetGap(1000)
                  .build();
            } catch (Exception e) {
              return null;
            }
          });
      return analyzerMap;
    }
  }

  @Test(expected = AnalyzerCreator.AnalyzerCreationException.class)
  public void testPluginAnalyzerNotDefined() {
    AnalyzerCreator.getInstance().getAnalyzer(getPredefinedAnalyzer("plugin_analyzer"));
  }

  @Test
  public void testPluginProvidesAnalyzer() throws IOException {
    init(Collections.singletonList(new TestAnalysisPlugin()));

    CustomAnalyzer analyzer =
        (CustomAnalyzer)
            AnalyzerCreator.getInstance().getAnalyzer(getPredefinedAnalyzer("plugin_analyzer"));
    assertHtmlStripClassicFolding(analyzer);
  }

  public static class MyTokenFilter extends LowerCaseFilterFactory {

    final Map<String, String> params;

    public MyTokenFilter(Map<String, String> params) {
      super(Collections.emptyMap());
      this.params = params;
    }
  }

  public static class TestTokenFilterPlugin extends Plugin implements AnalysisPlugin {
    @Override
    public Map<String, Class<? extends TokenFilterFactory>> getTokenFilters() {
      return Collections.singletonMap("test_token_filter", MyTokenFilter.class);
    }
  }

  @Test
  public void testPluginTokenFilterNotDefined() {
    try {
      AnalyzerCreator.getInstance()
          .getAnalyzer(
              com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder()
                  .setCustom(
                      com.yelp.nrtsearch.server.grpc.CustomAnalyzer.newBuilder()
                          .setTokenizer(NameAndParams.newBuilder().setName("keyword").build())
                          .addTokenFilters(
                              NameAndParams.newBuilder().setName("test_token_filter").build())
                          .build())
                  .build());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(
          e.getMessage()
              .contains("TokenFilterFactory with name 'test_token_filter' does not exist."));
    }
  }

  @Test
  public void testPluginProvidesTokenFilter() {
    init(Collections.singletonList(new TestTokenFilterPlugin()));

    CustomAnalyzer analyzer =
        (CustomAnalyzer)
            AnalyzerCreator.getInstance()
                .getAnalyzer(
                    com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder()
                        .setCustom(
                            com.yelp.nrtsearch.server.grpc.CustomAnalyzer.newBuilder()
                                .setTokenizer(NameAndParams.newBuilder().setName("keyword").build())
                                .addTokenFilters(
                                    NameAndParams.newBuilder().setName("test_token_filter").build())
                                .build())
                        .build());
    List<TokenFilterFactory> tokenFilters = analyzer.getTokenFilterFactories();
    assertEquals(1, tokenFilters.size());
    assertTrue(tokenFilters.get(0) instanceof MyTokenFilter);
    assertTrue(((MyTokenFilter) tokenFilters.get(0)).params.isEmpty());
  }

  @Test
  public void testTokenFilterParams() {
    init(Collections.singletonList(new TestTokenFilterPlugin()));

    Map<String, String> params = new HashMap<>();
    params.put("p1", "v1");
    params.put("p2", "v2");

    CustomAnalyzer analyzer =
        (CustomAnalyzer)
            AnalyzerCreator.getInstance()
                .getAnalyzer(
                    com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder()
                        .setCustom(
                            com.yelp.nrtsearch.server.grpc.CustomAnalyzer.newBuilder()
                                .setTokenizer(NameAndParams.newBuilder().setName("keyword").build())
                                .addTokenFilters(
                                    NameAndParams.newBuilder()
                                        .setName("test_token_filter")
                                        .putAllParams(params)
                                        .build())
                                .build())
                        .build());
    List<TokenFilterFactory> tokenFilters = analyzer.getTokenFilterFactories();
    assertEquals(1, tokenFilters.size());
    assertTrue(tokenFilters.get(0) instanceof MyTokenFilter);
    assertEquals(params, ((MyTokenFilter) tokenFilters.get(0)).params);
  }

  public static class TestTokenFilterDuplicatePlugin extends Plugin implements AnalysisPlugin {
    @Override
    public Map<String, Class<? extends TokenFilterFactory>> getTokenFilters() {
      return Collections.singletonMap("test_token_filter", ASCIIFoldingFilterFactory.class);
    }
  }

  @Test
  public void testTokenFilterDuplicatePluginRegistration() {
    try {
      init(List.of(new TestTokenFilterPlugin(), new TestTokenFilterDuplicatePlugin()));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Token filter test_token_filter already exists", e.getMessage());
    }
  }

  public static class TestTokenFilterDuplicateLucenePlugin extends Plugin
      implements AnalysisPlugin {
    @Override
    public Map<String, Class<? extends TokenFilterFactory>> getTokenFilters() {
      return Collections.singletonMap("asciifolding", MyTokenFilter.class);
    }
  }

  @Test
  public void testTokenFilterDuplicateLuceneRegistration() {
    try {
      init(Collections.singletonList(new TestTokenFilterDuplicateLucenePlugin()));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Token filter asciifolding already provided by lucene", e.getMessage());
    }
  }

  public static class MyBadTokenFilter extends LowerCaseFilterFactory {

    public MyBadTokenFilter(Map<String, String> args, int something) {
      super(args);
    }
  }

  public static class BadTokenFilterPlugin extends Plugin implements AnalysisPlugin {
    @Override
    public Map<String, Class<? extends TokenFilterFactory>> getTokenFilters() {
      return Collections.singletonMap("test_token_filter", MyBadTokenFilter.class);
    }
  }

  @Test
  public void testInvalidTokenFilterConstructor() {
    init(Collections.singletonList(new BadTokenFilterPlugin()));

    try {
      AnalyzerCreator.getInstance()
          .getAnalyzer(
              com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder()
                  .setCustom(
                      com.yelp.nrtsearch.server.grpc.CustomAnalyzer.newBuilder()
                          .setTokenizer(NameAndParams.newBuilder().setName("keyword").build())
                          .addTokenFilters(
                              NameAndParams.newBuilder().setName("test_token_filter").build())
                          .build())
                  .build());
      fail();
    } catch (UnsupportedOperationException e) {
      assertEquals(
          "Factory com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreatorTest$MyBadTokenFilter "
              + "cannot be instantiated. This is likely due to missing Map<String,String> constructor.",
          e.getMessage());
    }
  }

  public static class TokenFilterAnalysisComponent extends LowerCaseFilterFactory
      implements AnalysisComponent {
    LuceneServerConfiguration configuration;

    public TokenFilterAnalysisComponent(Map<String, String> args) {
      super(args);
    }

    @Override
    public void initializeComponent(LuceneServerConfiguration configuration) {
      this.configuration = configuration;
    }
  }

  public static class TokenFilterAnalysisComponentPlugin extends Plugin implements AnalysisPlugin {
    @Override
    public Map<String, Class<? extends TokenFilterFactory>> getTokenFilters() {
      return Collections.singletonMap("test_token_filter", TokenFilterAnalysisComponent.class);
    }
  }

  @Test
  public void testTokenFilterAnalysisComponent() {
    init(Collections.singletonList(new TokenFilterAnalysisComponentPlugin()));

    CustomAnalyzer analyzer =
        (CustomAnalyzer)
            AnalyzerCreator.getInstance()
                .getAnalyzer(
                    com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder()
                        .setCustom(
                            com.yelp.nrtsearch.server.grpc.CustomAnalyzer.newBuilder()
                                .setTokenizer(NameAndParams.newBuilder().setName("keyword").build())
                                .addTokenFilters(
                                    NameAndParams.newBuilder().setName("test_token_filter").build())
                                .build())
                        .build());
    List<TokenFilterFactory> tokenFilters = analyzer.getTokenFilterFactories();
    assertEquals(1, tokenFilters.size());
    assertTrue(tokenFilters.get(0) instanceof TokenFilterAnalysisComponent);
    assertNotNull(((TokenFilterAnalysisComponent) tokenFilters.get(0)).configuration);
  }
}
