package com.chase.app.analyzers;

import com.chase.app.tokenFilters.StopFilterHelper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class TextStemmingAnalyzer extends Analyzer { // TODO isnt there another place where we stem the text?
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer src = new StandardTokenizer();
      
      TokenStream res = new LowerCaseFilter(src);
      res = new ASCIIFoldingFilter(res);
      res = StopFilterHelper.UseEnglishStopFilter(res);
      res = new PorterStemFilter(res);

      return new TokenStreamComponents(src, res);
    }
  }