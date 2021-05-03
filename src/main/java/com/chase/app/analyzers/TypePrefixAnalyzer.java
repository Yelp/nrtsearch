package com.chase.app.analyzers;

import java.util.Arrays;

import com.chase.app.tokenFilters.EdgeNgramFilterHelper;
import com.chase.app.tokenFilters.WordDelimiterGraphFilterHelper;
import com.chase.app.tokenizers.PathsTokenizer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;

public class TypePrefixAnalyzer extends Analyzer {

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer src = new PathsTokenizer();
    
    TokenStream res = WordDelimiterGraphFilterHelper.UseDefaultWordDelimiterGraphSettings(src);
    res = new LowerCaseFilter(res);
    res = new ASCIIFoldingFilter(res);
    res = EdgeNgramFilterHelper.UsePrefixEdgeNgramFilter(res);  // TODO whats up with the resource leak warn?
   
    return new TokenStreamComponents(src, res);
  }
}
