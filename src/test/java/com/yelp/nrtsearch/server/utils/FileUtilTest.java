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
package com.yelp.nrtsearch.server.utils;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileUtilTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testDeleteAllFilesInDir_notExist() throws IOException {
    FileUtil.deleteAllFilesInDir(folder.getRoot().toPath().resolve("notExist"));
  }

  @Test
  public void testDeleteAllFilesInDir_notDir() throws IOException {
    folder.newFile("file");
    FileUtil.deleteAllFilesInDir(folder.getRoot().toPath().resolve("file"));
  }

  @Test
  public void testDeleteAllFilesInDir() throws IOException {
    folder.newFile("file1");
    folder.newFile("file2");
    folder.newFolder("folder1");
    folder.newFolder("folder1/folder2");
    FileUtil.deleteAllFilesInDir(folder.getRoot().toPath());
    assertArrayEquals(new String[] {}, folder.getRoot().list());
  }
}
