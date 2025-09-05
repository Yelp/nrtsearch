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
package com.yelp.nrtsearch.server.nrt;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.yelp.nrtsearch.server.grpc.FileMetadata;
import com.yelp.nrtsearch.server.grpc.FilesMetadata;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.junit.Test;

public class NrtUtilsTest {

  @Test
  public void testReadFilesMetaDataEmpty() throws IOException {
    FilesMetadata filesMetadata = FilesMetadata.newBuilder().setNumFiles(0).build();

    Map<String, FileMetaData> result = NrtUtils.readFilesMetaData(filesMetadata);

    assertThat(result).isEmpty();
  }

  @Test
  public void testReadFilesMetaDataSingleFile() throws IOException {
    byte[] header = {0x01, 0x02, 0x03, 0x04};
    byte[] footer = {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD};

    FileMetadata fileMetadata =
        FileMetadata.newBuilder()
            .setFileName("test.txt")
            .setLen(1024L)
            .setChecksum(123456789L)
            .setHeaderLength(header.length)
            .setHeader(ByteString.copyFrom(header))
            .setFooterLength(footer.length)
            .setFooter(ByteString.copyFrom(footer))
            .build();

    FilesMetadata filesMetadata =
        FilesMetadata.newBuilder().setNumFiles(1).addFileMetadata(fileMetadata).build();

    Map<String, FileMetaData> result = NrtUtils.readFilesMetaData(filesMetadata);

    assertThat(result).hasSize(1);
    assertThat(result).containsKey("test.txt");

    FileMetaData fileMetaData = result.get("test.txt");
    assertThat(fileMetaData.length()).isEqualTo(1024L);
    assertThat(fileMetaData.checksum()).isEqualTo(123456789L);
    assertThat(fileMetaData.header()).isEqualTo(header);
    assertThat(fileMetaData.footer()).isEqualTo(footer);
  }

  @Test
  public void testReadFilesMetaDataMultipleFiles() throws IOException {
    byte[] header1 = {0x10, 0x20};
    byte[] footer1 = {0x30, 0x40, 0x50};
    byte[] header2 = {(byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0xDD, (byte) 0xEE};
    byte[] footer2 = {(byte) 0x99};

    FileMetadata file1 =
        FileMetadata.newBuilder()
            .setFileName("file1.dat")
            .setLen(2048L)
            .setChecksum(111111111L)
            .setHeaderLength(header1.length)
            .setHeader(ByteString.copyFrom(header1))
            .setFooterLength(footer1.length)
            .setFooter(ByteString.copyFrom(footer1))
            .build();

    FileMetadata file2 =
        FileMetadata.newBuilder()
            .setFileName("file2.idx")
            .setLen(4096L)
            .setChecksum(222222222L)
            .setHeaderLength(header2.length)
            .setHeader(ByteString.copyFrom(header2))
            .setFooterLength(footer2.length)
            .setFooter(ByteString.copyFrom(footer2))
            .build();

    FilesMetadata filesMetadata =
        FilesMetadata.newBuilder()
            .setNumFiles(2)
            .addFileMetadata(file1)
            .addFileMetadata(file2)
            .build();

    Map<String, FileMetaData> result = NrtUtils.readFilesMetaData(filesMetadata);

    assertThat(result).hasSize(2);
    assertThat(result).containsKeys("file1.dat", "file2.idx");

    // Verify file1
    FileMetaData fileMetaData1 = result.get("file1.dat");
    assertThat(fileMetaData1.length()).isEqualTo(2048L);
    assertThat(fileMetaData1.checksum()).isEqualTo(111111111L);
    assertThat(fileMetaData1.header()).isEqualTo(header1);
    assertThat(fileMetaData1.footer()).isEqualTo(footer1);

    // Verify file2
    FileMetaData fileMetaData2 = result.get("file2.idx");
    assertThat(fileMetaData2.length()).isEqualTo(4096L);
    assertThat(fileMetaData2.checksum()).isEqualTo(222222222L);
    assertThat(fileMetaData2.header()).isEqualTo(header2);
    assertThat(fileMetaData2.footer()).isEqualTo(footer2);
  }

  @Test
  public void testReadFilesMetaDataWithEmptyHeaderAndFooter() throws IOException {
    byte[] emptyHeader = new byte[0];
    byte[] emptyFooter = new byte[0];

    FileMetadata fileMetadata =
        FileMetadata.newBuilder()
            .setFileName("empty_metadata.txt")
            .setLen(512L)
            .setChecksum(987654321L)
            .setHeaderLength(0)
            .setHeader(ByteString.copyFrom(emptyHeader))
            .setFooterLength(0)
            .setFooter(ByteString.copyFrom(emptyFooter))
            .build();

    FilesMetadata filesMetadata =
        FilesMetadata.newBuilder().setNumFiles(1).addFileMetadata(fileMetadata).build();

    Map<String, FileMetaData> result = NrtUtils.readFilesMetaData(filesMetadata);

    assertThat(result).hasSize(1);
    assertThat(result).containsKey("empty_metadata.txt");

    FileMetaData fileMetaData = result.get("empty_metadata.txt");
    assertThat(fileMetaData.length()).isEqualTo(512L);
    assertThat(fileMetaData.checksum()).isEqualTo(987654321L);
    assertThat(fileMetaData.header()).isEqualTo(emptyHeader);
    assertThat(fileMetaData.footer()).isEqualTo(emptyFooter);
  }

  @Test
  public void testReadFilesMetaDataWithZeroLength() throws IOException {
    byte[] header = {0x77, (byte) 0x88};
    byte[] footer = {(byte) 0x99, (byte) 0xAA, (byte) 0xBB};

    FileMetadata fileMetadata =
        FileMetadata.newBuilder()
            .setFileName("zero_length.dat")
            .setLen(0L)
            .setChecksum(0L)
            .setHeaderLength(header.length)
            .setHeader(ByteString.copyFrom(header))
            .setFooterLength(footer.length)
            .setFooter(ByteString.copyFrom(footer))
            .build();

    FilesMetadata filesMetadata =
        FilesMetadata.newBuilder().setNumFiles(1).addFileMetadata(fileMetadata).build();

    Map<String, FileMetaData> result = NrtUtils.readFilesMetaData(filesMetadata);

    assertThat(result).hasSize(1);
    assertThat(result).containsKey("zero_length.dat");

    FileMetaData fileMetaData = result.get("zero_length.dat");
    assertThat(fileMetaData.length()).isEqualTo(0L);
    assertThat(fileMetaData.checksum()).isEqualTo(0L);
    assertThat(fileMetaData.header()).isEqualTo(header);
    assertThat(fileMetaData.footer()).isEqualTo(footer);
  }

  @Test
  public void testReadFilesMetaDataWithNegativeChecksum() throws IOException {
    byte[] header = {0x01};
    byte[] footer = {0x02};

    FileMetadata fileMetadata =
        FileMetadata.newBuilder()
            .setFileName("negative_checksum.txt")
            .setLen(100L)
            .setChecksum(-123456789L)
            .setHeaderLength(header.length)
            .setHeader(ByteString.copyFrom(header))
            .setFooterLength(footer.length)
            .setFooter(ByteString.copyFrom(footer))
            .build();

    FilesMetadata filesMetadata =
        FilesMetadata.newBuilder().setNumFiles(1).addFileMetadata(fileMetadata).build();

    Map<String, FileMetaData> result = NrtUtils.readFilesMetaData(filesMetadata);

    assertThat(result).hasSize(1);
    assertThat(result).containsKey("negative_checksum.txt");

    FileMetaData fileMetaData = result.get("negative_checksum.txt");
    assertThat(fileMetaData.length()).isEqualTo(100L);
    assertThat(fileMetaData.checksum()).isEqualTo(-123456789L);
    assertThat(fileMetaData.header()).isEqualTo(header);
    assertThat(fileMetaData.footer()).isEqualTo(footer);
  }

  @Test
  public void testReadFilesMetaDataWithLargeFile() throws IOException {
    byte[] header = new byte[1024];
    byte[] footer = new byte[512];

    // Fill with some pattern
    for (int i = 0; i < header.length; i++) {
      header[i] = (byte) (i % 256);
    }
    for (int i = 0; i < footer.length; i++) {
      footer[i] = (byte) ((i * 2) % 256);
    }

    FileMetadata fileMetadata =
        FileMetadata.newBuilder()
            .setFileName("large_file.bin")
            .setLen(Long.MAX_VALUE)
            .setChecksum(Long.MAX_VALUE)
            .setHeaderLength(header.length)
            .setHeader(ByteString.copyFrom(header))
            .setFooterLength(footer.length)
            .setFooter(ByteString.copyFrom(footer))
            .build();

    FilesMetadata filesMetadata =
        FilesMetadata.newBuilder().setNumFiles(1).addFileMetadata(fileMetadata).build();

    Map<String, FileMetaData> result = NrtUtils.readFilesMetaData(filesMetadata);

    assertThat(result).hasSize(1);
    assertThat(result).containsKey("large_file.bin");

    FileMetaData fileMetaData = result.get("large_file.bin");
    assertThat(fileMetaData.length()).isEqualTo(Long.MAX_VALUE);
    assertThat(fileMetaData.checksum()).isEqualTo(Long.MAX_VALUE);
    assertThat(fileMetaData.header()).isEqualTo(header);
    assertThat(fileMetaData.footer()).isEqualTo(footer);
  }

  @Test
  public void testReadFilesMetaDataWithSpecialCharactersInFileName() throws IOException {
    byte[] header = {0x55};
    byte[] footer = {0x66};

    FileMetadata fileMetadata =
        FileMetadata.newBuilder()
            .setFileName("file with spaces & special chars!@#$%^&*()_+.txt")
            .setLen(256L)
            .setChecksum(555555555L)
            .setHeaderLength(header.length)
            .setHeader(ByteString.copyFrom(header))
            .setFooterLength(footer.length)
            .setFooter(ByteString.copyFrom(footer))
            .build();

    FilesMetadata filesMetadata =
        FilesMetadata.newBuilder().setNumFiles(1).addFileMetadata(fileMetadata).build();

    Map<String, FileMetaData> result = NrtUtils.readFilesMetaData(filesMetadata);

    assertThat(result).hasSize(1);
    assertThat(result).containsKey("file with spaces & special chars!@#$%^&*()_+.txt");

    FileMetaData fileMetaData = result.get("file with spaces & special chars!@#$%^&*()_+.txt");
    assertThat(fileMetaData.length()).isEqualTo(256L);
    assertThat(fileMetaData.checksum()).isEqualTo(555555555L);
    assertThat(fileMetaData.header()).isEqualTo(header);
    assertThat(fileMetaData.footer()).isEqualTo(footer);
  }

  @Test
  public void testReadFilesMetaDataConsistencyWithNumFiles() throws IOException {
    // Test that the assertion in the method works correctly
    byte[] header = {0x01};
    byte[] footer = {0x02};

    FileMetadata file1 =
        FileMetadata.newBuilder()
            .setFileName("file1.txt")
            .setLen(100L)
            .setChecksum(111L)
            .setHeaderLength(header.length)
            .setHeader(ByteString.copyFrom(header))
            .setFooterLength(footer.length)
            .setFooter(ByteString.copyFrom(footer))
            .build();

    FileMetadata file2 =
        FileMetadata.newBuilder()
            .setFileName("file2.txt")
            .setLen(200L)
            .setChecksum(222L)
            .setHeaderLength(header.length)
            .setHeader(ByteString.copyFrom(header))
            .setFooterLength(footer.length)
            .setFooter(ByteString.copyFrom(footer))
            .build();

    // numFiles matches actual count
    FilesMetadata filesMetadata =
        FilesMetadata.newBuilder()
            .setNumFiles(2)
            .addFileMetadata(file1)
            .addFileMetadata(file2)
            .build();

    Map<String, FileMetaData> result = NrtUtils.readFilesMetaData(filesMetadata);

    assertThat(result).hasSize(2);
    assertThat(result).containsKeys("file1.txt", "file2.txt");
  }

  @Test
  public void testReadFilesMetaDataPreservesOrder() throws IOException {
    byte[] header = {0x01};
    byte[] footer = {0x02};

    // Create files with predictable ordering
    FileMetadata[] files = new FileMetadata[5];
    for (int i = 0; i < 5; i++) {
      files[i] =
          FileMetadata.newBuilder()
              .setFileName("file_" + String.format("%02d", i) + ".txt")
              .setLen(100L + i)
              .setChecksum(1000L + i)
              .setHeaderLength(header.length)
              .setHeader(ByteString.copyFrom(header))
              .setFooterLength(footer.length)
              .setFooter(ByteString.copyFrom(footer))
              .build();
    }

    FilesMetadata.Builder builder = FilesMetadata.newBuilder().setNumFiles(5);

    for (FileMetadata file : files) {
      builder.addFileMetadata(file);
    }

    Map<String, FileMetaData> result = NrtUtils.readFilesMetaData(builder.build());

    assertThat(result).hasSize(5);

    // Verify all files are present with correct data
    for (int i = 0; i < 5; i++) {
      String fileName = "file_" + String.format("%02d", i) + ".txt";
      assertThat(result).containsKey(fileName);

      FileMetaData fileMetaData = result.get(fileName);
      assertThat(fileMetaData.length()).isEqualTo(100L + i);
      assertThat(fileMetaData.checksum()).isEqualTo(1000L + i);
      assertThat(fileMetaData.header()).isEqualTo(header);
      assertThat(fileMetaData.footer()).isEqualTo(footer);
    }
  }
}
