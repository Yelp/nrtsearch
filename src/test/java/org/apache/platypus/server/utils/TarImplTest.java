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

package org.apache.platypus.server.utils;

import com.amazonaws.util.IOUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TarImplTest {

    private TarEntry tarEntry1;
    private TarEntry tarEntry2;

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();
    private Path tarTestBaseDirectory;

    @Before
    public void setup() throws IOException {
        tarEntry1 = new TarEntry("/foo", "testcontentoffoo");
        tarEntry2 = new TarEntry("/bar/baz", "testcontentofbaz");
        tarTestBaseDirectory = folder.newFolder("tar").toPath();
    }

    @Test
    public void extractTar() throws IOException {
        Path sourceTarFile = tarTestBaseDirectory.resolve("test_tar.tar.gz");
        Path destDir = tarTestBaseDirectory.resolve("extractedDir");
        try (
                final FileOutputStream fileOutputStream = new FileOutputStream(sourceTarFile.toFile());
        ) {
            byte[] tarContent = getTarFile(Arrays.asList(tarEntry1, tarEntry2));
            fileOutputStream.write(tarContent);
        }
        new TarImpl().extractTar(sourceTarFile, destDir);

        assertEquals(Files.readAllLines(destDir.resolve("foo")).get(0), tarEntry1.content);
        assertEquals(Files.readAllLines(destDir.resolve("bar").resolve("baz")).get(0), tarEntry2.content);
    }

    @Test
    public void buildTar() throws IOException {
        Path sourceDir = Files.createDirectory(tarTestBaseDirectory.resolve("dirToTar"));
        Path subDir = Files.createDirectory(sourceDir.resolve("subDir"));
        try (
                ByteArrayInputStream test1content = new ByteArrayInputStream("test1content".getBytes());
                ByteArrayInputStream test2content = new ByteArrayInputStream("test2content".getBytes());
                FileOutputStream fileOutputStream1 = new FileOutputStream(sourceDir.resolve("test1").toFile());
                FileOutputStream fileOutputStream2 = new FileOutputStream(subDir.resolve("test2").toFile());
        ) {
            IOUtils.copy(test1content, fileOutputStream1);
            IOUtils.copy(test2content, fileOutputStream2);
        }
        Path destTarFile = tarTestBaseDirectory.resolve("result.tar.gz");
        new TarImpl().buildTar(sourceDir, destTarFile);

        try (
                final FileInputStream fileInputStream = new FileInputStream(destTarFile.toFile());
                final GzipCompressorInputStream gzipCompressorInputStream = new GzipCompressorInputStream(fileInputStream, true);
                final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(gzipCompressorInputStream);
        ) {
            Path destDir = tarTestBaseDirectory.resolve("test_extract");
            new TarImpl().extractTar(tarArchiveInputStream, destDir);
            assertEquals(true, dirsMatch(sourceDir.toFile(), destDir.resolve("dirToTar").toFile()));

        }

    }

    static boolean dirsMatch(File file1, File file2) throws IOException {
        if (file1.isDirectory() && file2.isDirectory()) {
            File[] files1 = file1.listFiles();
            File[] files2 = file2.listFiles();
            if (files1.length != files2.length) {
                return false;
            }
            for (int i = 0; i < files1.length; i++) {
                boolean isMatch = dirsMatch(files1[i], files2[i]);
                if (!isMatch) {
                    return false;
                }
            }
            return true;
        } else if (file1.isFile() && file2.isFile()) {
            return Files.readAllLines(file1.toPath()).get(0).equals(Files.readAllLines(file2.toPath()).get(0));
        }
        return false;
    }

    byte[] getTarFile(List<TarEntry> tarEntries) throws IOException {
        try (
                final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                final GzipCompressorOutputStream gzipCompressorOutputStream = new GzipCompressorOutputStream(byteArrayOutputStream);
                final TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(gzipCompressorOutputStream);
        ) {
            for (final TarEntry tarEntry : tarEntries) {
                final byte[] data = tarEntry.content.getBytes(StandardCharsets.UTF_8);
                final TarArchiveEntry archiveEntry = new TarArchiveEntry(tarEntry.path);
                archiveEntry.setSize(data.length);
                tarArchiveOutputStream.putArchiveEntry(archiveEntry);
                tarArchiveOutputStream.write(data);
                tarArchiveOutputStream.closeArchiveEntry();
            }

            tarArchiveOutputStream.close();
            return byteArrayOutputStream.toByteArray();
        }
    }

    class TarEntry {
        final public String path;
        final public String content;

        public TarEntry(String path, String content) {
            this.path = path;
            this.content = content;
        }
    }

}
