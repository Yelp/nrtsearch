/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.platypus.server.luceneserver;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
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
import org.apache.platypus.server.grpc.IntObject;
import org.apache.platypus.server.grpc.NameAndParams;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.lucene.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.apache.lucene.util.LuceneTestCase.random;
import static org.junit.Assert.*;

@RunWith(RandomizedRunner.class)   // Required to call org.apache.lucene.util.LuceneTestCase.random
public class AnalyzerCreatorTest {

    // Tests for predefined analyzers

    @Test
    public void testPredefinedStandardAnalyzer() {
        Analyzer analyzer = AnalyzerCreator.getAnalyzer(getPredefinedAnalyzer("standard"));

        assertSame(StandardAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testPredefinedClassicAnalyzer() {
        Analyzer analyzer = AnalyzerCreator.getAnalyzer(getPredefinedAnalyzer("classic"));

        assertSame(ClassicAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testPredefinedDynamicallyInitializedAnalyzer() {
        List<String> names = Arrays.asList("en.English", "bn.Bengali", "eu.Basque", "hy.Armenian", "ru.Russian", "th.Thai");
        List<Class> classes = Arrays.asList(EnglishAnalyzer.class, BengaliAnalyzer.class, BasqueAnalyzer.class,
                ArmenianAnalyzer.class, RussianAnalyzer.class, ThaiAnalyzer.class);

        assertEquals(names.size(), classes.size());

        for (int i = 0; i < names.size(); i++) {
            Analyzer analyzer = AnalyzerCreator.getAnalyzer(getPredefinedAnalyzer(names.get(i)));
            assertSame(classes.get(i), analyzer.getClass());
        }
    }

    private static org.apache.platypus.server.grpc.Analyzer getPredefinedAnalyzer(String name) {
        return org.apache.platypus.server.grpc.Analyzer.newBuilder()
                .setPredefined(name)
                .build();
    }

    // Tests for custom analyzers - created using tests in org.apache.lucene.analysis.custom.TestCustomAnalyzer

    @Test
    public void testCustomAnalyzerFactoryHtmlStripClassicFolding() throws IOException {
        org.apache.platypus.server.grpc.Analyzer analyzerGrpc = org.apache.platypus.server.grpc.Analyzer.newBuilder()
                .setCustom(org.apache.platypus.server.grpc.CustomAnalyzer.newBuilder()
                        .setDefaultMatchVersion("LATEST")
                        .addCharFilters(NameAndParams.newBuilder()
                                .setName("htmlstrip"))
                        .setTokenizer(NameAndParams.newBuilder()
                                .setName("classic"))
                        .addTokenFilters(NameAndParams.newBuilder()
                                .setName("asciifolding")
                                .putParams("preserveOriginal", "true"))
                        .addTokenFilters(NameAndParams.newBuilder()
                                .setName("lowercase"))
                        .setPositionIncrementGap(IntObject.newBuilder()
                                .setInt(100))
                        .setOffsetGap(IntObject.newBuilder()
                                .setInt(1000)))
                .build();

        CustomAnalyzer analyzer = (CustomAnalyzer) AnalyzerCreator.getAnalyzer(analyzerGrpc);

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

        assertAnalyzesTo(analyzer, "<p>foo bar</p> FOO BAR",
                new String[] { "foo", "bar", "foo", "bar" },
                new int[]    { 1,     1,     1,     1});
        assertAnalyzesTo(analyzer, "<p><b>föó</b> bär     FÖÖ BAR</p>",
                new String[] { "foo", "föó", "bar", "bär", "foo", "föö", "bar" },
                new int[]    { 1,     0,     1,     0,     1,     0,     1});
        analyzer.close();
    }

    public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int[] posIncrements) throws IOException {
        assertAnalyzesTo(a, input, output, null, null, null, posIncrements, null);
    }

    public static void assertAnalyzesTo(Analyzer a, String input, String[] output, int startOffsets[], int endOffsets[], String types[], int posIncrements[], int posLengths[]) throws IOException {
        checkResetException(a, input);
        BaseTokenStreamTestCase.checkAnalysisConsistency(random(), a, true, input);
        assertTokenStreamContents(a.tokenStream("dummy", input), output, startOffsets, endOffsets, types, posIncrements, posLengths, input.length());
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
            while (ts.incrementToken()) { }
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
        org.apache.platypus.server.grpc.Analyzer analyzerGrpc = org.apache.platypus.server.grpc.Analyzer.newBuilder()
                .setCustom(org.apache.platypus.server.grpc.CustomAnalyzer.newBuilder()
                        .setTokenizer(NameAndParams.newBuilder()
                                .setName("whitespace"))
                        .addTokenFilters(NameAndParams.newBuilder()
                                .setName("asciifolding"))
                        .addTokenFilters(NameAndParams.newBuilder()
                                .setName("lowercase")))
                .build();

        CustomAnalyzer analyzer = (CustomAnalyzer) AnalyzerCreator.getAnalyzer(analyzerGrpc);

        assertEquals(new BytesRef("a b e"), analyzer.normalize("dummy", "À B é"));
    }

    @Test
    public void testCustomAnalyzerNormalizationWithMultiplCharFilters() {
        // none of these components are multi-term aware so they should not be applied
        org.apache.platypus.server.grpc.Analyzer analyzerGrpc = org.apache.platypus.server.grpc.Analyzer.newBuilder()
                .setCustom(org.apache.platypus.server.grpc.CustomAnalyzer.newBuilder()
                        .addCharFilters(NameAndParams.newBuilder()
                                .setName("mapping")
                                .putParams("mapping", "custom_analyzer_mapping/mapping1.txt"))
                        .addCharFilters(NameAndParams.newBuilder()
                                .setName("mapping")
                                .putParams("mapping", "custom_analyzer_mapping/mapping2.txt"))
                        .setTokenizer(NameAndParams.newBuilder()
                                .setName("whitespace")))
                .build();

        CustomAnalyzer analyzer = (CustomAnalyzer) AnalyzerCreator.getAnalyzer(analyzerGrpc);

        assertEquals(new BytesRef("e f c"), analyzer.normalize("dummy", "a b c"));
    }
}