package com.chase.app.search;

import org.apache.lucene.analysis.Analyzer;

public class FieldMetadata {

    public FieldMetadata(String name, String extension, Boolean isAnalyzed, Analyzer analyzer) {
        this(name, extension, isAnalyzed, analyzer, null);
    }
    public FieldMetadata(String name, Boolean isAnalyzed, Analyzer analyzer) {
        this(name, null, isAnalyzed, analyzer, null);
    }
    public FieldMetadata(String name, Analyzer analyzer, Analyzer searchAnalyzer) {
        this(name, null, true, analyzer, searchAnalyzer);
    }
    public FieldMetadata(String name, String ext, Analyzer analyzer, Analyzer searchAnalyzer) {
        this(name, ext, true, analyzer, searchAnalyzer);
    }
    public FieldMetadata(String name, Analyzer analyzer) {
        this(name, null, true, analyzer, null);
    }
    public FieldMetadata(String name, String ext, Analyzer analyzer) {
        this(name, ext, true, analyzer, null);
    }
    public FieldMetadata(String name) {
        this(name, null, false, null, null);
    }
    public FieldMetadata(String name, String extension) {
        this(name, extension, false, null, null);
    }


    public FieldMetadata(String name, String extension, Boolean isAnalyzed, Analyzer analyzer, Analyzer searchAnalyzer) {
        this.Analyzed = isAnalyzed;
        this.Name = name;
        this.Extension = extension;
        this.Analyzer = analyzer;
        this.SearchAnalyzer = searchAnalyzer;
    }
    public Analyzer Analyzer;
    public Analyzer SearchAnalyzer = null;
    public String Name;
    public String Extension = null;
    public Boolean Analyzed;
    public String FullName() {
        return Extension == null ? Name : String.format("%s.%s", Name, Extension);
    } 
}