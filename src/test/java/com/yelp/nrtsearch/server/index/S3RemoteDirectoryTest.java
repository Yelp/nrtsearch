/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.index;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.junit.Before;
import org.junit.Test;

public class S3RemoteDirectoryTest {
  private AmazonS3 mockS3Client;
  private S3RemoteDirectory s3RemoteDirectory;
  private String bucket = "test-bucket";
  private String prefix = "test-prefix/";

  @Before
  public void setup() {
    mockS3Client = mock(AmazonS3.class);
    s3RemoteDirectory = new S3RemoteDirectory(mockS3Client, bucket, new RemoteFilePath(prefix));
  }

  @Test
  public void testOpenInput_success() throws IOException {
    String fileName = "foo.dat";
    byte[] fileData = "my-mock-segment-data".getBytes();

    // Mocks for S3 object
    S3Object mockS3Object = mock(S3Object.class);
    ObjectMetadata mockMetadata = mock(ObjectMetadata.class);
    S3ObjectInputStream mockS3ObjectInputStream =
        new S3ObjectInputStream(new ByteArrayInputStream(fileData), null);

    String key = new RemoteFilePath(prefix).getPath() + fileName;
    when(mockS3Client.doesObjectExist(bucket, key)).thenReturn(true);
    when(mockS3Client.getObject(bucket, key)).thenReturn(mockS3Object);
    when(mockS3Object.getObjectContent()).thenReturn(mockS3ObjectInputStream);
    when(mockS3Client.getObjectMetadata(bucket, key)).thenReturn(mockMetadata);
    when(mockMetadata.getContentLength()).thenReturn((long) fileData.length);

    try (IndexInput input = s3RemoteDirectory.openInput(fileName, IOContext.DEFAULT)) {
      byte[] buf = new byte[fileData.length];
      input.readBytes(buf, 0, buf.length);
      assertThat(buf).isEqualTo(fileData);
    }
  }

  @Test
  public void testOpenInput_fileDoesNotExist() {
    String fileName = "missing.dat";
    String key = prefix + fileName;
    when(mockS3Client.doesObjectExist(bucket, key)).thenReturn(false);

    assertThatThrownBy(() -> s3RemoteDirectory.openInput(fileName, IOContext.DEFAULT))
        .isInstanceOf(NoSuchFileException.class)
        .hasMessageContaining(fileName);
  }
}
