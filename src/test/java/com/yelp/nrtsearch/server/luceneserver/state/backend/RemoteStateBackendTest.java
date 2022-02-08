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
package com.yelp.nrtsearch.server.luceneserver.state.backend;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.backup.BackupDiffManager;
import com.yelp.nrtsearch.server.backup.ContentDownloader;
import com.yelp.nrtsearch.server.backup.ContentDownloaderImpl;
import com.yelp.nrtsearch.server.backup.FileCompressAndUploader;
import com.yelp.nrtsearch.server.backup.IndexArchiver;
import com.yelp.nrtsearch.server.backup.TarImpl;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.PersistentGlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.PersistentGlobalState.IndexInfo;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import io.findify.s3mock.S3Mock;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RemoteStateBackendTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static final String TEST_BUCKET = "remote-state-test";
  private static final String TEST_SERVICE_NAME = "test-service-name";
  private S3Mock api;
  private VersionManager versionManager;
  private Archiver archiver;

  @Before
  public void setup() throws IOException {
    Path s3Directory = folder.newFolder("s3").toPath();
    Path archiverDirectory = folder.newFolder("archiver").toPath();

    api = S3Mock.create(8011, s3Directory.toAbsolutePath().toString());
    api.start();
    AmazonS3 s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint("http://127.0.0.1:8011");
    s3.createBucket(TEST_BUCKET);
    TransferManager transferManager =
        TransferManagerBuilder.standard().withS3Client(s3).withShutDownThreadPools(false).build();

    ContentDownloader contentDownloader =
        new ContentDownloaderImpl(
            new TarImpl(TarImpl.CompressionMode.LZ4), transferManager, TEST_BUCKET, true);
    FileCompressAndUploader fileCompressAndUploader =
        new FileCompressAndUploader(
            new TarImpl(TarImpl.CompressionMode.LZ4), transferManager, TEST_BUCKET);
    versionManager = new VersionManager(s3, TEST_BUCKET);
    archiver =
        new IndexArchiver(
            mock(BackupDiffManager.class),
            fileCompressAndUploader,
            contentDownloader,
            versionManager,
            archiverDirectory);
  }

  @After
  public void teardown() {
    api.shutdown();
  }

  private LuceneServerConfiguration getConfig(boolean readOnly) throws IOException {
    String configFile =
        String.join(
            "\n",
            "stateConfig:",
            "  backendType: REMOTE",
            "  remote:",
            "    readOnly: " + readOnly,
            "stateDir: " + folder.getRoot().getAbsolutePath(),
            "serviceName: " + TEST_SERVICE_NAME);
    return new LuceneServerConfiguration(new ByteArrayInputStream(configFile.getBytes()));
  }

  private GlobalState getMockGlobalState(boolean readOnly) throws IOException {
    GlobalState mockState = mock(GlobalState.class);
    LuceneServerConfiguration serverConfiguration = getConfig(readOnly);
    when(mockState.getConfiguration()).thenReturn(serverConfiguration);
    when(mockState.getStateDir()).thenReturn(Paths.get(serverConfiguration.getStateDir()));
    when(mockState.getIncArchiver()).thenReturn(Optional.of(archiver));
    return mockState;
  }

  private Path getLocalStateFilePath() {
    return Paths.get(
        folder.getRoot().getAbsolutePath(),
        StateUtils.GLOBAL_STATE_FOLDER,
        StateUtils.GLOBAL_STATE_FILE);
  }

  private Path getS3FilePath(String versionHash) {
    return Paths.get(
        folder.getRoot().getAbsolutePath(),
        "s3",
        TEST_BUCKET,
        TEST_SERVICE_NAME,
        RemoteStateBackend.GLOBAL_STATE_RESOURCE,
        versionHash);
  }

  private PersistentGlobalState getS3State() throws IOException {
    long currentVersion =
        versionManager.getLatestVersionNumber(
            TEST_SERVICE_NAME, RemoteStateBackend.GLOBAL_STATE_RESOURCE);
    if (currentVersion < 0) {
      return null;
    }
    String versionHash =
        versionManager.getVersionString(
            TEST_SERVICE_NAME,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            String.valueOf(currentVersion));
    Path s3FilePath = getS3FilePath(versionHash);
    assertTrue(s3FilePath.toFile().exists());
    assertTrue(s3FilePath.toFile().isFile());

    TarArchiveInputStream tarArchiveInputStream =
        new TarArchiveInputStream(
            new LZ4FrameInputStream(new FileInputStream(s3FilePath.toFile())));
    PersistentGlobalState stateFromTar = null;
    for (TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
        tarArchiveEntry != null;
        tarArchiveEntry = tarArchiveInputStream.getNextTarEntry()) {
      if (tarArchiveEntry.getName().endsWith(StateUtils.GLOBAL_STATE_FILE)) {
        String stateStr;
        try (DataInputStream dataInputStream = new DataInputStream(tarArchiveInputStream)) {
          stateStr = dataInputStream.readUTF();
        }
        stateFromTar = StateUtils.MAPPER.readValue(stateStr, PersistentGlobalState.class);
        break;
      }
    }
    return stateFromTar;
  }

  private void writeStateToS3(PersistentGlobalState state) throws IOException {
    File tmpFolderFile = folder.newFolder();
    Path tmpGlobalStatePath =
        Paths.get(tmpFolderFile.getAbsolutePath(), StateUtils.GLOBAL_STATE_FOLDER);
    StateUtils.ensureDirectory(tmpGlobalStatePath);
    StateUtils.writeStateToFile(state, tmpGlobalStatePath, StateUtils.GLOBAL_STATE_FILE);
    String version =
        archiver.upload(
            TEST_SERVICE_NAME,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            tmpGlobalStatePath,
            Collections.singletonList(StateUtils.GLOBAL_STATE_FILE),
            Collections.emptyList(),
            true);
    archiver.blessVersion(TEST_SERVICE_NAME, RemoteStateBackend.GLOBAL_STATE_RESOURCE, version);
  }

  @Test
  public void testCreatesLocalStateDir() throws IOException {
    Path stateDir = Paths.get(folder.getRoot().getAbsolutePath(), StateUtils.GLOBAL_STATE_FOLDER);
    assertFalse(stateDir.toFile().exists());

    new RemoteStateBackend(getMockGlobalState(false));
    assertTrue(stateDir.toFile().exists());
    assertTrue(stateDir.toFile().isDirectory());
  }

  @Test
  public void testCreatesDefaultState() throws IOException {
    StateBackend stateBackend = new RemoteStateBackend(getMockGlobalState(false));
    Path localFilePath = getLocalStateFilePath();
    assertFalse(localFilePath.toFile().exists());
    assertNull(getS3State());

    PersistentGlobalState globalState = stateBackend.loadOrCreateGlobalState();
    assertEquals(globalState, new PersistentGlobalState());

    assertTrue(localFilePath.toFile().exists());
    assertTrue(localFilePath.toFile().isFile());

    PersistentGlobalState loadedLocalState = StateUtils.readStateFromFile(localFilePath);
    assertEquals(globalState, loadedLocalState);

    PersistentGlobalState stateFromTar = getS3State();
    assertNotNull(stateFromTar);
    assertEquals(globalState, stateFromTar);
  }

  @Test
  public void testLoadsSavedState() throws IOException {
    StateBackend stateBackend = new RemoteStateBackend(getMockGlobalState(false));
    Path localFilePath = getLocalStateFilePath();
    assertFalse(localFilePath.toFile().exists());

    Map<String, IndexInfo> indicesMap = new HashMap<>();
    indicesMap.put("test_index", new IndexInfo());
    indicesMap.put("test_index_2", new IndexInfo());
    PersistentGlobalState initialState = new PersistentGlobalState(indicesMap);

    writeStateToS3(initialState);

    PersistentGlobalState loadedState = stateBackend.loadOrCreateGlobalState();
    assertEquals(initialState, loadedState);

    assertTrue(localFilePath.toFile().exists());
    assertTrue(localFilePath.toFile().isFile());

    PersistentGlobalState loadedLocalState = StateUtils.readStateFromFile(localFilePath);
    assertEquals(initialState, loadedLocalState);
  }

  @Test
  public void testCommitGlobalState() throws IOException {
    StateBackend stateBackend = new RemoteStateBackend(getMockGlobalState(false));
    Path localFilePath = getLocalStateFilePath();
    PersistentGlobalState initialState = stateBackend.loadOrCreateGlobalState();

    Map<String, IndexInfo> indicesMap = new HashMap<>();
    indicesMap.put("test_index", new IndexInfo());
    indicesMap.put("test_index_2", new IndexInfo());
    PersistentGlobalState updatedState = new PersistentGlobalState(indicesMap);
    assertNotEquals(initialState, updatedState);

    stateBackend.commitGlobalState(updatedState);
    PersistentGlobalState loadedState = getS3State();
    assertEquals(updatedState, loadedState);
    PersistentGlobalState loadedLocalState = StateUtils.readStateFromFile(localFilePath);
    assertEquals(updatedState, loadedLocalState);

    indicesMap = new HashMap<>();
    indicesMap.put("test_index_3", new IndexInfo());
    PersistentGlobalState updatedState2 = new PersistentGlobalState(indicesMap);
    assertNotEquals(updatedState, updatedState2);
    stateBackend.commitGlobalState(updatedState2);

    loadedState = getS3State();
    assertEquals(updatedState2, loadedState);
    loadedLocalState = StateUtils.readStateFromFile(localFilePath);
    assertEquals(updatedState2, loadedLocalState);
  }

  @Test(expected = NullPointerException.class)
  public void testCommitNullState() throws IOException {
    StateBackend stateBackend = new RemoteStateBackend(getMockGlobalState(false));
    stateBackend.loadOrCreateGlobalState();
    stateBackend.commitGlobalState(null);
  }

  @Test
  public void testReadOnlyNoInitialState() throws IOException {
    StateBackend stateBackend = new RemoteStateBackend(getMockGlobalState(true));
    assertNull(getS3State());
    try {
      stateBackend.loadOrCreateGlobalState();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Cannot update remote state when configured as read only", e.getMessage());
    }
  }

  @Test
  public void testReadOnlyWithInitialState() throws IOException {
    StateBackend stateBackend = new RemoteStateBackend(getMockGlobalState(true));

    Map<String, IndexInfo> indicesMap = new HashMap<>();
    indicesMap.put("test_index", new IndexInfo());
    indicesMap.put("test_index_2", new IndexInfo());
    PersistentGlobalState initialState = new PersistentGlobalState(indicesMap);

    writeStateToS3(initialState);
    PersistentGlobalState loadedState = stateBackend.loadOrCreateGlobalState();
    assertEquals(initialState, loadedState);
  }

  @Test
  public void testReadOnlyCommit() throws IOException {
    StateBackend stateBackend = new RemoteStateBackend(getMockGlobalState(true));

    Map<String, IndexInfo> indicesMap = new HashMap<>();
    indicesMap.put("test_index", new IndexInfo());
    indicesMap.put("test_index_2", new IndexInfo());
    PersistentGlobalState initialState = new PersistentGlobalState(indicesMap);

    writeStateToS3(initialState);
    PersistentGlobalState loadedState = stateBackend.loadOrCreateGlobalState();
    assertEquals(initialState, loadedState);

    indicesMap = new HashMap<>();
    indicesMap.put("test_index_3", new IndexInfo());
    indicesMap.put("test_index_4", new IndexInfo());
    indicesMap.put("test_index_5", new IndexInfo());
    PersistentGlobalState updatedState = new PersistentGlobalState(indicesMap);
    try {
      stateBackend.commitGlobalState(updatedState);
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Cannot update remote state when configured as read only", e.getMessage());
    }
  }

  @Test
  public void testArchiverRequired() throws IOException {
    GlobalState mockState = mock(GlobalState.class);
    LuceneServerConfiguration serverConfiguration = getConfig(false);
    when(mockState.getConfiguration()).thenReturn(serverConfiguration);
    when(mockState.getStateDir()).thenReturn(Paths.get(serverConfiguration.getStateDir()));
    try {
      new RemoteStateBackend(mockState);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Archiver must be provided for remote state usage", e.getMessage());
    }
  }
}
