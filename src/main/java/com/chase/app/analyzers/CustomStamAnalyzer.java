package com.chase.app.analyzers;

import com.chase.app.tokenizers.PathsTokenizer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.Tokenizer;

public class CustomStamAnalyzer extends Analyzer {

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer src = new PathsTokenizer(); //new WhitespaceTokenizer();
        TokenStream res = new LengthFilter(src, 3, Integer.MAX_VALUE);
        return new TokenStreamComponents(src, res);
    }
}