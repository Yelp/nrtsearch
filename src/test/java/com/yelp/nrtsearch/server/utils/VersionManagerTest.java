/*
 *
 *  *
 *  *  Copyright 2019 Yelp Inc.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  *  either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *
 *
 *
 */

package com.yelp.nrtsearch.server.utils;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class VersionManagerTest {
    private final String BUCKET_NAME = "version-manager-unittest";
    private VersionManager versionManager;
    private S3Mock api;
    private AmazonS3 s3;
    private Path s3Directory;
    private Path archiverDirectory;

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        s3Directory = folder.newFolder("s3").toPath();
        archiverDirectory = folder.newFolder("version-manager").toPath();

        api = S3Mock.create(8011, s3Directory.toAbsolutePath().toString());
        api.start();
        s3 = new AmazonS3Client(new AnonymousAWSCredentials());
        s3.setEndpoint("http://127.0.0.1:8011");
        s3.createBucket(BUCKET_NAME);
        versionManager = new VersionManager(s3, BUCKET_NAME);

    }

    @After
    public void teardown() {
        api.shutdown();
    }

    @Test
    public void blessVersionNoResourceHash() {
        boolean result = versionManager.blessVersion("testservice", "testresource", "abcdef");
        assertEquals(false, result);
    }

    @Test
    public void  blessVersionWhenNoPrior() throws IOException {
        s3.putObject(BUCKET_NAME, "testservice/testresource/abcdef", "foo");
        boolean result = versionManager.blessVersion("testservice", "testresource", "abcdef");
        assertEquals(true, result);

        S3Object s3Object = s3.getObject(BUCKET_NAME, "testservice/_version/testresource/_latest_version");
        assertEquals("0", IOUtils.toString(s3Object.getObjectContent()));

        s3Object = s3.getObject(BUCKET_NAME, "testservice/_version/testresource/0");
        assertEquals("abcdef", IOUtils.toString(s3Object.getObjectContent()));

    }

    @Test
    public void  blessVersionWhenPrior() throws IOException {
        s3.putObject(BUCKET_NAME, "testservice/testresource/abcdef", "foo");
        s3.putObject(BUCKET_NAME, "testservice/_version/testresource/_latest_version", "0");
        boolean result = versionManager.blessVersion("testservice", "testresource", "abcdef");
        assertEquals(true, result);

        S3Object s3Object = s3.getObject(BUCKET_NAME, "testservice/_version/testresource/_latest_version");
        assertEquals("1", IOUtils.toString(s3Object.getObjectContent()));

        s3Object = s3.getObject(BUCKET_NAME, "testservice/_version/testresource/1");
        assertEquals("abcdef", IOUtils.toString(s3Object.getObjectContent()));
    }

    @Test
    public void blessVersionWhenLatestVersionBehind() throws IOException {
        s3.putObject(BUCKET_NAME, "testservice/testresource/abcdef", "foo");
        s3.putObject(BUCKET_NAME, "testservice/_version/testresource/_latest_version", "0");
        s3.putObject(BUCKET_NAME, "testservice/_version/testresource/1", "ghijkl");

        boolean result = versionManager.blessVersion("testservice", "testresource", "abcdef");
        assertEquals(true, result);

        S3Object s3Object = s3.getObject(BUCKET_NAME, "testservice/_version/testresource/_latest_version");
        assertEquals("2", IOUtils.toString(s3Object.getObjectContent()));

        s3Object = s3.getObject(BUCKET_NAME, "testservice/_version/testresource/2");
        assertEquals("abcdef", IOUtils.toString(s3Object.getObjectContent()));

    }


}