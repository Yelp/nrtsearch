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
package com.yelp.nrtsearch.server.backup;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BackupDiffComputerTest {
  public static class BackupDiffInfoTest {
    @Test
    public void getBackupDiffInfo() {
      Set<String> oldFileNames = Set.of("1", "2", "3", "4");
      Set<String> currentFileNames = Set.of("1", "4", "5");
      BackupDiffComputer.BackupDiffInfo backupDiffInfo =
          BackupDiffComputer.BackupDiffInfo.generateBackupDiffInfo(oldFileNames, currentFileNames);
      assertEquals(Set.of("5"), backupDiffInfo.getToBeAdded());
      assertEquals(Set.of("1", "4"), backupDiffInfo.getAlreadyUploaded());
      assertEquals(Set.of("2", "3"), backupDiffInfo.getToBeRemoved());
    }
  }

  public static class TempDirManagerTest {
    @Rule public final TemporaryFolder folder = new TemporaryFolder();
    private Path someTempDir;

    @Before
    public void setUp() throws IOException {
      someTempDir = folder.newFolder("some_temp_dir").toPath();
    }

    @Test
    public void tempDirCreateAndDelete() throws Exception {
      Path tempDir;
      try (BackupDiffComputer.TempDirManager tempDirManager =
          new BackupDiffComputer.TempDirManager(someTempDir)) {

        Path tempDirPath = Paths.get(tempDirManager.getPath().toString());
        Path someTempFile = Paths.get(tempDirPath.toString(), "someTempFile");
        assertEquals(false, Files.exists(tempDirPath));
        Files.createDirectory(tempDirPath);
        Files.createFile(someTempFile);
        assertEquals(true, Files.exists(tempDirPath));
        assertEquals(true, Files.exists(someTempFile));
        tempDir = tempDirManager.getPath();
      }
      assertEquals(false, Files.exists(tempDir));
    }

    @Test
    public void tempDirDelete() throws Exception {
      Path tempDir;
      try (BackupDiffComputer.TempDirManager tempDirManager =
          new BackupDiffComputer.TempDirManager(someTempDir)) {

        Path tempDirPath = Paths.get(tempDirManager.getPath().toString());
        assertEquals(false, Files.exists(tempDirPath));
        tempDir = tempDirManager.getPath();
      }
      assertEquals(false, Files.exists(tempDir));
    }
  }

  public static class BackupDiffMarshallerTest {
    @Rule public final TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testSerDe() throws IOException {
      List<String> indexFileNames = List.of("1", "2", "3");
      final Path tempFile = folder.newFile("tempFile.txt").toPath();
      BackupDiffComputer.BackupDiffMarshaller.serializeFileNames(indexFileNames, tempFile);
      assertEquals(
          indexFileNames, BackupDiffComputer.BackupDiffMarshaller.deserializeFileNames(tempFile));
    }
  }

  public static class BackupDiffDirValidatorTest {
    @Rule public final TemporaryFolder folder = new TemporaryFolder();

    @Test(expected = RuntimeException.class)
    public void baseDirDoesNotExist() throws IOException {
      Path path = folder.newFolder().toPath().resolve("temp");
      BackupDiffComputer.BackupDiffDirValidator.validateMetaFile(path);
    }

    @Test(expected = RuntimeException.class)
    public void baseDirHasSubDir() throws IOException {
      Path path = folder.newFolder("subDir").toPath();
      Path parentDir = path.getParent();
      Files.createFile(Paths.get(parentDir.toString(), "file1"));
      BackupDiffComputer.BackupDiffDirValidator.validateMetaFile(parentDir);
    }

    @Test(expected = RuntimeException.class)
    public void baseDirHasMultipleFiles() throws IOException {
      File f1 = folder.newFile("file1");
      folder.newFile("file2");
      Path parentDir = f1.toPath().getParent();
      BackupDiffComputer.BackupDiffDirValidator.validateMetaFile(parentDir);
    }

    @Test
    public void baseDirHasOneFile() throws IOException {
      Path f1 = folder.newFile("file1").toPath();
      Files.writeString(f1, "contents", StandardCharsets.UTF_8);
      Path parentDir = f1.getParent();
      Path fileName = BackupDiffComputer.BackupDiffDirValidator.validateMetaFile(parentDir);
      assertEquals("contents", new String(Files.readAllBytes(fileName)));
    }
  }
}
