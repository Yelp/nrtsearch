/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver;

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.utils.Archiver;
import com.yelp.nrtsearch.server.utils.ArchiverImpl;
import com.yelp.nrtsearch.server.utils.TarEntry;
import com.yelp.nrtsearch.server.utils.TarImpl;
import io.findify.s3mock.S3Mock;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RestoreStateHandlerTest {
  private final String BUCKET_NAME = "archiver-unittest";
  private Archiver archiver;
  private S3Mock api;
  private AmazonS3 s3;
  private Path s3Directory;
  private Path archiverDirectory;
  private GlobalState globalState;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    s3Directory = folder.newFolder("s3").toPath();
    archiverDirectory = folder.newFolder("archiver").toPath();

    api = S3Mock.create(8011, s3Directory.toAbsolutePath().toString());
    api.start();
    s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint("http://127.0.0.1:8011");
    s3.createBucket(BUCKET_NAME);
    archiver =
        new ArchiverImpl(
            s3, BUCKET_NAME, archiverDirectory, new TarImpl(TarImpl.CompressionMode.LZ4));
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    globalState = new GlobalState(luceneServerConfiguration);
  }

  @After
  public void teardown() throws IOException {
    api.shutdown();
    rmDir(globalState.getStateDir());
  }

  @Test
  public void handleBadServiceName() throws IOException {
    List<String> indexes = RestoreStateHandler.restore(archiver, globalState, "NoSuchService");
    Assert.assertEquals(0, indexes.size());
  }

  @Test
  public void handleNoResource() throws IOException {
    s3.putObject(BUCKET_NAME, "testservice/_version/testresource/_latest_version", "1");
    s3.putObject(BUCKET_NAME, "testservice/_version/testresource/1", "abcdef");
    List<String> indexes = RestoreStateHandler.restore(archiver, globalState, "testservice");
    Assert.assertEquals(0, indexes.size());
  }

  @Test
  public void handleOneResource() throws IOException {
    s3.putObject(BUCKET_NAME, "testservice/_version/testresource_metadata/_latest_version", "1");
    s3.putObject(BUCKET_NAME, "testservice/_version/testresource_metadata/1", "abcdef");
    final TarEntry tarEntry = new TarEntry("foo", "testcontent");
    TarEntry.uploadToS3(
        s3, BUCKET_NAME, Arrays.asList(tarEntry), "testservice/testresource_metadata/abcdef");
    List<String> indexes = RestoreStateHandler.restore(archiver, globalState, "testservice");
    Assert.assertEquals(1, indexes.size());
    Assert.assertEquals("testresource_metadata", indexes.get(0));
  }
}
