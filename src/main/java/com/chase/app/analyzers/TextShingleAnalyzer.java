package com.chase.app.analyzers;

import com.chase.app.tokenFilters.ShingleFilterHelper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class TextShingleAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer src = new StandardTokenizer();
      
      TokenStream res = new LowerCaseFilter(src);
      res = new ASCIIFoldingFilter(res);
      res = new PorterStemFilter(res);
      res = ShingleFilterHelper.UseTrigramFilter(res);  
        // TODO would allow strange shingles since we use standard tokenizer, maybe we should change to whitespace tokenizer.
      res = ShingleFilterHelper.UseTrigramLengthLimitFilter(res);
      return new TokenStreamComponents(src, res);
    }
}