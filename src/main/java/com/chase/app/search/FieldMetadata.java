/*
 * Copyright 2021 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

  public FieldMetadata(
      String name,
      String extension,
      Boolean isAnalyzed,
      Analyzer analyzer,
      Analyzer searchAnalyzer) {
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
