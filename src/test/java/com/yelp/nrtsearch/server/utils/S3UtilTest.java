/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.utils;

import static com.yelp.nrtsearch.server.utils.S3Util.getS3FileName;
import static com.yelp.nrtsearch.server.utils.S3Util.isValidS3FilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class S3UtilTest {

  @Test
  public void testIsValidS3FilePath() {
    assertTrue(isValidS3FilePath("s3://random-bucket/path1"));
    assertTrue(isValidS3FilePath("s3://random-bucket/path1/path2"));
    assertTrue(isValidS3FilePath("s3://random-bucket/path1/path2/path3.txt"));
    assertFalse(isValidS3FilePath("s3://random-bucket/path1/path2/"));
    assertFalse(isValidS3FilePath("s3://random-bucket"));
    assertFalse(isValidS3FilePath("/random-bucket/path1"));
    assertFalse(isValidS3FilePath("/random-bucket/path1/path2"));
    assertFalse(isValidS3FilePath("/random-bucket/path1/path2/path3.txt"));
    assertFalse(isValidS3FilePath("C://random-bucket/path1/path2/path3.txt"));
    assertFalse(isValidS3FilePath("~/random-bucket/path1/path2/path3.txt"));
    assertFalse(isValidS3FilePath("http://random-bucket/path1/path2/path3.txt"));
  }

  @Test
  public void testGetS3FileName() {
    assertEquals("path1", getS3FileName("s3://random-bucket/path1"));
    assertEquals("path2", getS3FileName("s3://random-bucket/path1/path2"));
    assertEquals("path3.txt", getS3FileName("s3://random-bucket/path1/path2/path3.txt"));
    assertThrows(IllegalArgumentException.class, () -> getS3FileName("s3://random-bucket"));
    assertThrows(
        IllegalArgumentException.class, () -> getS3FileName("s3://random-bucket/path1/path2/"));
  }
}
