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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.apache.platypus.server.grpc.ConditionalTokenFilter;
import org.apache.platypus.server.grpc.Field;
import org.apache.platypus.server.grpc.NameAndParams;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.HashMap;

public class AnalyzerCreator {

    private static final String LUCENE_ANALYZER_PATH = "org.apache.lucene.analysis.{0}Analyzer";
    private static final String STANDARD = "standard";
    private static final String CLASSIC = "classic";

    static Analyzer getAnalyzer(org.apache.platypus.server.grpc.Analyzer analyzer) {
        if (!analyzer.getPredefined().isEmpty()) {
            String predefinedAnalyzer = analyzer.getPredefined();

            if (STANDARD.equals(predefinedAnalyzer)) {
                return new StandardAnalyzer();
            } else if (CLASSIC.equals(predefinedAnalyzer)) {
                return new ClassicAnalyzer();
            } else {
                // Try to dynamically load the analyzer class
                try {
                    String className = MessageFormat.format(LUCENE_ANALYZER_PATH, predefinedAnalyzer);
                    return (Analyzer) AnalyzerCreator.class.getClassLoader().loadClass(className).getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | ClassNotFoundException | InvocationTargetException e) {
                    throw new AnalyzerCreationException("Unable to find predefined analyzer: " + predefinedAnalyzer, e);
                }
            }
        } else if (analyzer.hasCustom()) {
            return getCustomAnalyzer(analyzer.getCustom());
        } else {
            throw new AnalyzerCreationException("Unable to find or create analyzer: " + analyzer);
        }
    }

    /**
     * Create an {@link Analyzer} from user parameters. Note that we create new maps with the param maps because
     * the Protobuf one may be unmodifiable and Lucene may modify the maps.
     */
    private static Analyzer getCustomAnalyzer(org.apache.platypus.server.grpc.CustomAnalyzer analyzer) {
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

            builder.withTokenizer(analyzer.getTokenizer().getName(), new HashMap<>(analyzer.getTokenizer().getParamsMap()));

            for (NameAndParams tokenFilter : analyzer.getTokenFiltersList()) {
                builder.addTokenFilter(tokenFilter.getName(), new HashMap<>(tokenFilter.getParamsMap()));
            }

            // TODO: The only impl of ConditionalTokenFilter is ProtectedTermFilter (https://lucene.apache.org/core/8_2_0/analyzers-common/org/apache/lucene/analysis/miscellaneous/ProtectedTermFilterFactory.html)
            // It needs a protected terms file as input which is not supported yet.
            for (ConditionalTokenFilter conditionalTokenFilter : analyzer.getConditionalTokenFiltersList()) {
                NameAndParams condition = conditionalTokenFilter.getCondition();
                CustomAnalyzer.ConditionBuilder when = builder.when(condition.getName(), condition.getParamsMap());

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
    static Analyzer getStandardAnalyzer() {
        return new StandardAnalyzer();
    }

    static boolean hasAnalyzer(Field field) {
        return field != null && (isAnalyzerDefined(field.getAnalyzer()) || isAnalyzerDefined(field.getIndexAnalyzer())
                || isAnalyzerDefined(field.getSearchAnalyzer()));
    }

    static boolean isAnalyzerDefined(org.apache.platypus.server.grpc.Analyzer analyzer) {
        return analyzer != null
                && (!analyzer.getPredefined().isEmpty() || analyzer.hasCustom());
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
