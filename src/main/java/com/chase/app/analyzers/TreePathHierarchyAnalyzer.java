package io.chaseapp.localsearch.analyzers;
​
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
​
public class TreePathHierarchyAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new PathHierarchyTokenizer('\u0019', '\u0019'));
    }
}