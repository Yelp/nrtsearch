package com.yelp.nrtsearch.server.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TarEntry {
    final public String path;
    final public String content;

    public TarEntry(String path, String content) {
        this.path = path;
        this.content = content;
    }

    public static void uploadToS3(AmazonS3 s3, String bucketName, List<TarEntry> tarEntries, String key) throws IOException {
        byte[] tarContent = getTarFile(tarEntries);
        final ObjectMetadata objectMetadata = new ObjectMetadata();

        try (final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(tarContent)) {
            s3.putObject(bucketName, key, byteArrayInputStream, objectMetadata);
        }
    }

    private static byte[] getTarFile(List<TarEntry> tarEntries) throws IOException {
        try (
                final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                final LZ4FrameOutputStream lz4CompressorOutputStream = new LZ4FrameOutputStream(byteArrayOutputStream);
                final TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(lz4CompressorOutputStream);
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

}