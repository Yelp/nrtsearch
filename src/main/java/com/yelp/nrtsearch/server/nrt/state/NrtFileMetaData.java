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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData.NrtFileMetaDataDeserializer;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.replicator.nrt.FileMetaData;

/**
 * Replacement for {@link FileMetaData} that includes additional metadata for NRT replication, such
 * as primaryId and timeString.
 */
@JsonDeserialize(using = NrtFileMetaDataDeserializer.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NrtFileMetaData {

  public byte[] header;
  public byte[] footer;
  public long length;
  public long checksum;
  public String primaryId;
  public String timeString;

  /**
   * Constructor for NrtFileMetaData.
   *
   * @param header file header
   * @param footer file footer
   * @param length file length
   * @param checksum file checksum
   * @param primaryId primary id
   * @param timeString time string
   */
  public NrtFileMetaData(
      byte[] header,
      byte[] footer,
      long length,
      long checksum,
      String primaryId,
      String timeString) {
    this.header = header;
    this.footer = footer;
    this.length = length;
    this.checksum = checksum;
    this.primaryId = primaryId;
    this.timeString = timeString;
  }

  /**
   * Constructor for NrtFileMetaData from lucene FileMetaData.
   *
   * @param metaData lucene FileMetaData
   * @param primaryId primary id
   * @param timeString time string
   */
  public NrtFileMetaData(FileMetaData metaData, String primaryId, String timeString) {
    this.header = metaData.header();
    this.footer = metaData.footer();
    this.length = metaData.length();
    this.checksum = metaData.checksum();
    this.primaryId = primaryId;
    this.timeString = timeString;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof NrtFileMetaData other) {
      return length == other.length
          && checksum == other.checksum
          && Objects.equals(primaryId, other.primaryId)
          && Objects.equals(timeString, other.timeString)
          && Arrays.equals(header, other.header)
          && Arrays.equals(footer, other.footer);
    }
    return false;
  }

  public FileMetaData toFileMetaData() {
    return new FileMetaData(header, footer, length, checksum);
  }

  /** Custom json deserializer for NrtFileMetaData. */
  public static class NrtFileMetaDataDeserializer extends StdDeserializer<NrtFileMetaData> {

    public NrtFileMetaDataDeserializer() {
      this(null);
    }

    public NrtFileMetaDataDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public NrtFileMetaData deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = jp.getCodec().readTree(jp);
      byte[] header = node.get("header").binaryValue();
      byte[] footer = node.get("footer").binaryValue();
      long length = node.get("length").longValue();
      long checksum = node.get("checksum").longValue();
      String pid = node.get("primaryId").textValue();
      String timeString = node.get("timeString").textValue();
      return new NrtFileMetaData(header, footer, length, checksum, pid, timeString);
    }
  }
}
