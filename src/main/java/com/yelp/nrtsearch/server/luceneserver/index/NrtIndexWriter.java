/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.index;

import com.yelp.nrtsearch.server.monitoring.IndexMetrics;
import java.io.IOException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

/** IndexWriter that allows for collection of additional metrics. */
public class NrtIndexWriter extends IndexWriter {
  private final String indexName;

  /**
   * Constructs a new IndexWriter per the settings given in <code>conf</code>. If you want to make
   * "live" changes to this writer instance, use {@link #getConfig()}.
   *
   * <p><b>NOTE:</b> after ths writer is created, the given configuration instance cannot be passed
   * to another writer.
   *
   * @param d the index directory. The index is either created or appended according <code>
   *     conf.getOpenMode()</code>.
   * @param conf the configuration settings according to which IndexWriter should be initialized.
   * @throws IOException if the directory cannot be read/written to, or if it does not exist and
   *     <code>conf.getOpenMode()</code> is <code>OpenMode.APPEND</code> or if there is any other
   *     low-level IO error
   */
  public NrtIndexWriter(Directory d, IndexWriterConfig conf, String indexName) throws IOException {
    super(d, conf);
    this.indexName = indexName;
  }

  /**
   * A hook for extending classes to execute operations before pending added and deleted documents
   * are flushed to the Directory.
   */
  @Override
  protected void doBeforeFlush() throws IOException {
    IndexMetrics.flushCount.labels(indexName).inc();
  }
}
