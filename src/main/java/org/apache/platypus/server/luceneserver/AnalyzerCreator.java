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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.apache.platypus.server.grpc.*;

import java.io.IOException;
import java.text.ParseException;

public class AnalyzerCreator {

    static Analyzer getAnalyzer(IndexState state, org.apache.platypus.server.grpc.Analyzer analyzer) {
        if (!analyzer.getPredefined().isEmpty()) {
            //TODO: support all analyzers from lucene-analysis, CJK, and  CustomAnalyzers
            return new StandardAnalyzer();
        } else if (analyzer.hasCustom()) {
            return getCustomAnalyzer(analyzer.getCustom());
        } else {
            return null;
        }
    }

    private static Analyzer getCustomAnalyzer(org.apache.platypus.server.grpc.CustomAnalyzer analyzer) {
        CustomAnalyzer.Builder builder = CustomAnalyzer.builder();

        if (analyzer.hasPositionIncrementGap()) {
            builder.withPositionIncrementGap(analyzer.getPositionIncrementGap().getPositionIncrementGap());
        }
        if (analyzer.hasOffsetGap()) {
            builder.withOffsetGap(analyzer.getOffsetGap().getOffsetGap());
        }

        try {
            if (analyzer.hasDefaultMatchVersion()) {
                builder.withDefaultMatchVersion(Version.parseLeniently(analyzer.getDefaultMatchVersion().getVersion()));
            }

            for (NameAndParams charFilter : analyzer.getCharFiltersList()) {
                builder.addCharFilter(charFilter.getName(), charFilter.getParamsMap());
            }

            builder.withTokenizer(analyzer.getTokenizer().getName(), analyzer.getTokenizer().getParamsMap());

            for (NameAndParams tokenFilter : analyzer.getTokenFiltersList()) {
                builder.addTokenFilter(tokenFilter.getName(), tokenFilter.getParamsMap());
            }

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
            throw new AnalyzerCreationException("Unable to create custom analyzer", e);
        }
    }

    static Analyzer getStandardAnalyzer() {
        return new StandardAnalyzer();
    }

    static boolean hasAnalyzer(Field field) {
        return !field.getAnalyzer().getPredefined().isEmpty() || field.getAnalyzer().hasCustom();
    }

    static class AnalyzerCreationException extends RuntimeException {

        AnalyzerCreationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
