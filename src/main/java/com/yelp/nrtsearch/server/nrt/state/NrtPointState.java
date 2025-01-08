/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.nrt.state;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;

/** State of a single NRT point, including the files and metadata associated with it. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class NrtPointState {

  public Map<String, NrtFileMetaData> files;
  public long version;
  public long gen;
  public byte[] infosBytes;
  public long primaryGen;
  public Set<String> completedMergeFiles;
  public String primaryId;

  public NrtPointState() {}

  /**
   * Constructor for NrtPointState.
   *
   * @param copyState lucene CopyState
   * @param files map of file name to NrtFileMetaData
   * @param primaryId primary id
   */
  public NrtPointState(CopyState copyState, Map<String, NrtFileMetaData> files, String primaryId) {
    version = copyState.version();
    gen = copyState.gen();
    infosBytes = copyState.infosBytes();
    primaryGen = copyState.primaryGen();
    completedMergeFiles = copyState.completedMergeFiles();
    this.files = files;
    this.primaryId = primaryId;
  }

  /**
   * Convert this NrtPointState to a lucene CopyState.
   *
   * @return lucene CopyState
   */
  @JsonIgnore
  public CopyState toCopyState() {
    Map<String, FileMetaData> luceneFiles =
        files.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().toFileMetaData()));
    return new CopyState(
        luceneFiles, version, gen, infosBytes, completedMergeFiles, primaryGen, null);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof NrtPointState other) {
      return version == other.version
          && gen == other.gen
          && primaryGen == other.primaryGen
          && Objects.equals(primaryId, other.primaryId)
          && Objects.equals(completedMergeFiles, other.completedMergeFiles)
          && Arrays.equals(infosBytes, other.infosBytes)
          && Objects.equals(files, other.files);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("NrtPointState(%s, %d, %d, %d)", primaryId, primaryGen, gen, version);
  }
}
