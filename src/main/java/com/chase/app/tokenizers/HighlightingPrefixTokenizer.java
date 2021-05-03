package com.chase.app.tokenizers;

import java.util.Set;

import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.util.AttributeFactory;

// like https://github.com/chaseappio/backend-resources-service/blob/dev/src/Chase.Resources.Elastic/Template/Tokenizers/PathTokenizer.cs
public class HighlightingPrefixTokenizer extends EdgeNGramTokenizer {

    private static final int MINGRAM_SIZE = 3;
    private static final int MAXGRAM_SIZE = 20;

    public HighlightingPrefixTokenizer() {
        super(MINGRAM_SIZE, MAXGRAM_SIZE);
    }

    public HighlightingPrefixTokenizer(AttributeFactory factory) {
        super(factory, MINGRAM_SIZE, MAXGRAM_SIZE);
    }
    

    @Override
    protected boolean isTokenChar(int c) {
        // Support letters, numbers, punctuations or symbols. According to 
        // https://github.com/elastic/elasticsearch/blob/a92a647b9f17d1bddf5c707490a19482c273eda3/modules/analysis-common/src/main/java/org/elasticsearch/analysis/common/CharMatcher.java

        if (Character.isLetter(c) ||
            Character.isDigit(c))
            return true;

        int ctype = Character.getType(c);
        return  ctype == Character.START_PUNCTUATION || 
                ctype == Character.END_PUNCTUATION ||
                ctype == Character.OTHER_PUNCTUATION ||
                ctype == Character.CONNECTOR_PUNCTUATION ||
                ctype == Character.DASH_PUNCTUATION ||
                ctype == Character.INITIAL_QUOTE_PUNCTUATION ||
                ctype == Character.FINAL_QUOTE_PUNCTUATION ||
                ctype == Character.CURRENCY_SYMBOL ||
                ctype == Character.MATH_SYMBOL ||
                ctype == Character.OTHER_SYMBOL ||
                ctype == Character.MODIFIER_SYMBOL;
    }
    
}