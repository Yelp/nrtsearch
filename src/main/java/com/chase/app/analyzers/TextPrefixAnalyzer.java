package com.chase.app.analyzers;

import com.chase.app.tokenFilters.EdgeNgramFilterHelper;
import com.chase.app.tokenFilters.ShingleFilterHelper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class TextPrefixAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer src = new StandardTokenizer();
      
      TokenStream res = new LowerCaseFilter(src);
      res = new ASCIIFoldingFilter(res);
      res = ShingleFilterHelper.UseTrigramFilter(res); // TODO why are we using this here as well?
      res = EdgeNgramFilterHelper.UsePrefixEdgeNgramFilter(res);
      res = new RemoveDuplicatesTokenFilter(res);

      return new TokenStreamComponents(src, res);
    }
  }
