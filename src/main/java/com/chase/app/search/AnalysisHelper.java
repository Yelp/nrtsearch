package com.chase.app.search;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;


import com.chase.app.search.TokenData;

public class AnalysisHelper {
    public static ArrayList<TokenData> Analyze(String fieldName, String text, Analyzer analyzer) throws IOException
    {
        ArrayList<TokenData> ret = new ArrayList<TokenData>();
        try (TokenStream ts = analyzer.tokenStream(fieldName, text)) {
            OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            int pos = 0;
            PositionIncrementAttribute posAtt = ts.addAttribute(PositionIncrementAttribute.class);
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);

            ts.reset();

            while (ts.incrementToken()) {
                pos = pos + posAtt.getPositionIncrement();
                ret.add(new TokenData(termAtt.toString(), offsetAtt.startOffset(), offsetAtt.endOffset(), pos));
            }
            ts.end();
        }

        return ret;
    }
}