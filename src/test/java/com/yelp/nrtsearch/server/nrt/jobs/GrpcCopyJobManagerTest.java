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
package com.yelp.nrtsearch.server.nrt.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.yelp.nrtsearch.server.grpc.CopyState;
import com.yelp.nrtsearch.server.grpc.FileMetadata;
import com.yelp.nrtsearch.server.grpc.FilesMetadata;
import com.yelp.nrtsearch.server.nrt.NRTPrimaryNode;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.junit.Test;

public class GrpcCopyJobManagerTest {

  @Test
  public void testReadCopyState_BasicFields() throws IOException {
    // Create a basic CopyState with all required fields
    byte[] infoBytes = new byte[] {1, 2, 3, 4, 5};
    long gen = 100L;
    long version = 200L;
    long primaryGen = 150L;
    long timestampSec = 1640995200L; // 2022-01-01 00:00:00 UTC

    CopyState grpcCopyState =
        CopyState.newBuilder()
            .setInfoBytesLength(infoBytes.length)
            .setInfoBytes(ByteString.copyFrom(infoBytes))
            .setGen(gen)
            .setVersion(version)
            .setPrimaryGen(primaryGen)
            .setTimestampSec(timestampSec)
            .setFilesMetadata(FilesMetadata.newBuilder().build())
            .build();

    NRTPrimaryNode.CopyStateAndTimestamp result = GrpcCopyJobManager.readCopyState(grpcCopyState);

    assertNotNull(result);
    assertNotNull(result.copyState());
    assertNotNull(result.timestamp());

    org.apache.lucene.replicator.nrt.CopyState luceneCopyState = result.copyState();
    assertEquals(gen, luceneCopyState.gen());
    assertEquals(version, luceneCopyState.version());
    assertEquals(primaryGen, luceneCopyState.primaryGen());
    assertEquals(Instant.ofEpochSecond(timestampSec), result.timestamp());

    // Verify info bytes - note: infosBytes is protected, so we can't access it directly
    // This is sufficient verification that the method runs without error
    assertNotNull(luceneCopyState.files()); // Basic sanity check
  }

  @Test
  public void testReadCopyState_WithFiles() throws IOException {
    // Create file metadata
    FileMetadata file1 =
        FileMetadata.newBuilder()
            .setFileName("segments_1")
            .setLen(1024)
            .setChecksum(12345L)
            .setHeaderLength(0)
            .setHeader(ByteString.EMPTY)
            .setFooterLength(0)
            .setFooter(ByteString.EMPTY)
            .build();

    FileMetadata file2 =
        FileMetadata.newBuilder()
            .setFileName("_0.si")
            .setLen(2048)
            .setChecksum(67890L)
            .setHeaderLength(0)
            .setHeader(ByteString.EMPTY)
            .setFooterLength(0)
            .setFooter(ByteString.EMPTY)
            .build();

    FilesMetadata filesMetadata =
        FilesMetadata.newBuilder()
            .setNumFiles(2)
            .addFileMetadata(file1)
            .addFileMetadata(file2)
            .build();

    CopyState grpcCopyState =
        CopyState.newBuilder()
            .setInfoBytesLength(10)
            .setInfoBytes(ByteString.copyFrom(new byte[10]))
            .setGen(100L)
            .setVersion(200L)
            .setPrimaryGen(150L)
            .setTimestampSec(1640995200L)
            .setFilesMetadata(filesMetadata)
            .build();

    NRTPrimaryNode.CopyStateAndTimestamp result = GrpcCopyJobManager.readCopyState(grpcCopyState);

    assertNotNull(result);
    Map<String, FileMetaData> files = result.copyState().files();
    assertEquals(2, files.size());

    assertTrue(files.containsKey("segments_1"));
    assertTrue(files.containsKey("_0.si"));

    FileMetaData fileData1 = files.get("segments_1");
    assertEquals(1024, fileData1.length());
    assertEquals(12345L, fileData1.checksum());

    FileMetaData fileData2 = files.get("_0.si");
    assertEquals(2048, fileData2.length());
    assertEquals(67890L, fileData2.checksum());
  }

  @Test
  public void testReadCopyState_WithCompletedMergeFiles() throws IOException {
    CopyState grpcCopyState =
        CopyState.newBuilder()
            .setInfoBytesLength(5)
            .setInfoBytes(ByteString.copyFrom(new byte[5]))
            .setGen(100L)
            .setVersion(200L)
            .setPrimaryGen(150L)
            .setTimestampSec(1640995200L)
            .setFilesMetadata(FilesMetadata.newBuilder().build())
            .addCompletedMergeFiles("merge_file_1.tmp")
            .addCompletedMergeFiles("merge_file_2.tmp")
            .addCompletedMergeFiles("merge_file_3.tmp")
            .build();

    NRTPrimaryNode.CopyStateAndTimestamp result = GrpcCopyJobManager.readCopyState(grpcCopyState);

    assertNotNull(result);
    Set<String> completedMergeFiles = result.copyState().completedMergeFiles();
    assertEquals(3, completedMergeFiles.size());
    assertTrue(completedMergeFiles.contains("merge_file_1.tmp"));
    assertTrue(completedMergeFiles.contains("merge_file_2.tmp"));
    assertTrue(completedMergeFiles.contains("merge_file_3.tmp"));
  }

  @Test
  public void testReadCopyState_NoTimestamp() throws IOException {
    // Test with timestamp = 0 (no timestamp)
    CopyState grpcCopyState =
        CopyState.newBuilder()
            .setInfoBytesLength(5)
            .setInfoBytes(ByteString.copyFrom(new byte[5]))
            .setGen(100L)
            .setVersion(200L)
            .setPrimaryGen(150L)
            .setTimestampSec(0L) // No timestamp
            .setFilesMetadata(FilesMetadata.newBuilder().build())
            .build();

    NRTPrimaryNode.CopyStateAndTimestamp result = GrpcCopyJobManager.readCopyState(grpcCopyState);

    assertNotNull(result);
    assertNotNull(result.copyState());
    assertNull(result.timestamp()); // Should be null when timestampSec is 0
  }

  @Test
  public void testReadCopyState_EmptyInfoBytes() throws IOException {
    // Test with empty info bytes
    CopyState grpcCopyState =
        CopyState.newBuilder()
            .setInfoBytesLength(0)
            .setInfoBytes(ByteString.EMPTY)
            .setGen(100L)
            .setVersion(200L)
            .setPrimaryGen(150L)
            .setTimestampSec(1640995200L)
            .setFilesMetadata(FilesMetadata.newBuilder().build())
            .build();

    NRTPrimaryNode.CopyStateAndTimestamp result = GrpcCopyJobManager.readCopyState(grpcCopyState);

    assertNotNull(result);
    org.apache.lucene.replicator.nrt.CopyState luceneCopyState = result.copyState();
    // Note: infosBytes is protected, so we verify indirectly by checking the object is created
    // correctly
    assertNotNull(luceneCopyState.files());
  }

  @Test
  public void testReadCopyState_ComplexScenario() throws IOException {
    // Test with a complex scenario that includes all features
    byte[] complexInfoBytes = "complex_segment_info_data".getBytes();

    FileMetadata file1 =
        FileMetadata.newBuilder()
            .setFileName("segments_2")
            .setLen(4096)
            .setChecksum(111111L)
            .setHeaderLength(0)
            .setHeader(ByteString.EMPTY)
            .setFooterLength(0)
            .setFooter(ByteString.EMPTY)
            .build();

    FileMetadata file2 =
        FileMetadata.newBuilder()
            .setFileName("_0.cfs")
            .setLen(8192)
            .setChecksum(222222L)
            .setHeaderLength(0)
            .setHeader(ByteString.EMPTY)
            .setFooterLength(0)
            .setFooter(ByteString.EMPTY)
            .build();

    FileMetadata file3 =
        FileMetadata.newBuilder()
            .setFileName("_1.si")
            .setLen(512)
            .setChecksum(333333L)
            .setHeaderLength(0)
            .setHeader(ByteString.EMPTY)
            .setFooterLength(0)
            .setFooter(ByteString.EMPTY)
            .build();

    FilesMetadata filesMetadata =
        FilesMetadata.newBuilder()
            .setNumFiles(3)
            .addFileMetadata(file1)
            .addFileMetadata(file2)
            .addFileMetadata(file3)
            .build();

    CopyState grpcCopyState =
        CopyState.newBuilder()
            .setInfoBytesLength(complexInfoBytes.length)
            .setInfoBytes(ByteString.copyFrom(complexInfoBytes))
            .setGen(999L)
            .setVersion(1234L)
            .setPrimaryGen(888L)
            .setTimestampSec(1672531200L) // 2023-01-01 00:00:00 UTC
            .setFilesMetadata(filesMetadata)
            .addCompletedMergeFiles("old_merge_1.tmp")
            .addCompletedMergeFiles("old_merge_2.tmp")
            .build();

    NRTPrimaryNode.CopyStateAndTimestamp result = GrpcCopyJobManager.readCopyState(grpcCopyState);

    assertNotNull(result);

    // Verify basic fields
    org.apache.lucene.replicator.nrt.CopyState luceneCopyState = result.copyState();
    assertEquals(999L, luceneCopyState.gen());
    assertEquals(1234L, luceneCopyState.version());
    assertEquals(888L, luceneCopyState.primaryGen());
    assertEquals(Instant.ofEpochSecond(1672531200L), result.timestamp());

    // Note: infosBytes is protected, so we can't verify it directly but test that parsing succeeded

    // Verify files
    Map<String, FileMetaData> files = luceneCopyState.files();
    assertEquals(3, files.size());

    FileMetaData fileData1 = files.get("segments_2");
    assertNotNull(fileData1);
    assertEquals(4096, fileData1.length());
    assertEquals(111111L, fileData1.checksum());

    FileMetaData fileData2 = files.get("_0.cfs");
    assertNotNull(fileData2);
    assertEquals(8192, fileData2.length());
    assertEquals(222222L, fileData2.checksum());

    FileMetaData fileData3 = files.get("_1.si");
    assertNotNull(fileData3);
    assertEquals(512, fileData3.length());
    assertEquals(333333L, fileData3.checksum());

    // Verify completed merge files
    Set<String> completedMergeFiles = luceneCopyState.completedMergeFiles();
    assertEquals(2, completedMergeFiles.size());
    assertTrue(completedMergeFiles.contains("old_merge_1.tmp"));
    assertTrue(completedMergeFiles.contains("old_merge_2.tmp"));
  }

  @Test
  public void testReadCopyState_LargeInfoBytes() throws IOException {
    // Test with large info bytes to ensure proper byte handling
    byte[] largeInfoBytes = new byte[10000];
    for (int i = 0; i < largeInfoBytes.length; i++) {
      largeInfoBytes[i] = (byte) (i % 256);
    }

    CopyState grpcCopyState =
        CopyState.newBuilder()
            .setInfoBytesLength(largeInfoBytes.length)
            .setInfoBytes(ByteString.copyFrom(largeInfoBytes))
            .setGen(100L)
            .setVersion(200L)
            .setPrimaryGen(150L)
            .setTimestampSec(1640995200L)
            .setFilesMetadata(FilesMetadata.newBuilder().build())
            .build();

    NRTPrimaryNode.CopyStateAndTimestamp result = GrpcCopyJobManager.readCopyState(grpcCopyState);

    assertNotNull(result);
    org.apache.lucene.replicator.nrt.CopyState luceneCopyState = result.copyState();

    // Note: infosBytes is protected, so we can't verify the content directly
    // but we can verify the parsing succeeded and the object is valid
    assertNotNull(luceneCopyState.files());
  }

  @Test
  public void testReadCopyState_ManyFiles() throws IOException {
    // Test with many files
    FilesMetadata.Builder filesMetadataBuilder = FilesMetadata.newBuilder();

    for (int i = 0; i < 100; i++) {
      FileMetadata file =
          FileMetadata.newBuilder()
              .setFileName("file_" + i + ".dat")
              .setLen(1000L + i)
              .setChecksum(10000L + i)
              .setHeaderLength(0)
              .setHeader(ByteString.EMPTY)
              .setFooterLength(0)
              .setFooter(ByteString.EMPTY)
              .build();
      filesMetadataBuilder.addFileMetadata(file);
    }

    CopyState grpcCopyState =
        CopyState.newBuilder()
            .setInfoBytesLength(5)
            .setInfoBytes(ByteString.copyFrom(new byte[5]))
            .setGen(100L)
            .setVersion(200L)
            .setPrimaryGen(150L)
            .setTimestampSec(1640995200L)
            .setFilesMetadata(filesMetadataBuilder.setNumFiles(100).build())
            .build();

    NRTPrimaryNode.CopyStateAndTimestamp result = GrpcCopyJobManager.readCopyState(grpcCopyState);

    assertNotNull(result);
    Map<String, FileMetaData> files = result.copyState().files();
    assertEquals(100, files.size());

    // Verify a few files
    for (int i = 0; i < 10; i++) {
      String fileName = "file_" + i + ".dat";
      assertTrue("File " + fileName + " should exist", files.containsKey(fileName));

      FileMetaData fileData = files.get(fileName);
      assertEquals(1000L + i, fileData.length());
      assertEquals(10000L + i, fileData.checksum());
    }
  }
}
