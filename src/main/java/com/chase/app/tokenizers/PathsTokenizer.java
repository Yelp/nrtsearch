package com.chase.app.tokenizers;

import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.AttributeFactory;

// like https://github.com/chaseappio/backend-resources-service/blob/dev/src/Chase.Resources.Elastic/Template/Tokenizers/PathTokenizer.cs
public class PathsTokenizer extends CharTokenizer {
    public PathsTokenizer() {
        super();
    }

    public PathsTokenizer(AttributeFactory factory) {
        super(factory);
    }


    @Override
    protected boolean isTokenChar(int arg0) {
        char c = (char) arg0;
        return !(Character.isWhitespace(c) || c == ':' || c == '.' || c == '/' || c == '-' || c == '_');
    }
}