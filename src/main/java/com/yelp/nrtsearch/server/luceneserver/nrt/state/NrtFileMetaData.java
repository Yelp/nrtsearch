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
package com.yelp.nrtsearch.server.luceneserver.nrt.state;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtFileMetaData.NrtFileMetaDataDeserializer;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.replicator.nrt.FileMetaData;

/**
 * Extension of {@link FileMetaData} that includes additional metadata for NRT replication, such as
 * primaryId and timeString.
 */
@JsonDeserialize(using = NrtFileMetaDataDeserializer.class)
public class NrtFileMetaData extends FileMetaData {

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
    super(header, footer, length, checksum);
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
    super(metaData.header, metaData.footer, metaData.length, metaData.checksum);
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
