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
package com.yelp.nrtsearch;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.inject.*;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.yelp.nrtsearch.server.backup.*;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;

public class ArchiverModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ContentDownloader.class)
        .annotatedWith(Names.named("contentDownloaderNoTar"))
        .toProvider(ContentDownloaderNoTar.class)
        .asEagerSingleton();
    bind(ContentDownloader.class)
        .annotatedWith(Names.named("contentDownloaderWithTar"))
        .toProvider(ContentDownloaderWithTar.class)
        .asEagerSingleton();
    bind(FileCompressAndUploader.class)
        .annotatedWith(Names.named("fileCompressAndUploaderNoTar"))
        .toProvider(FileCompressAndUploaderNoTar.class)
        .asEagerSingleton();
    bind(FileCompressAndUploader.class)
        .annotatedWith(Names.named("fileCompressAndUploaderWithTar"))
        .toProvider(FileCompressAndUploaderWithTar.class)
        .asEagerSingleton();
    bind(Archiver.class)
        .annotatedWith(Names.named("legacyArchiver"))
        .toProvider(LegacyArchiverProvider.class)
        .asEagerSingleton();
    bind(BackupDiffManager.class).toProvider(BackupDiffManagerProvider.class).asEagerSingleton();
    bind(Archiver.class)
        .annotatedWith(Names.named("incArchiver"))
        .toProvider(IncArchiverProvider.class)
        .asEagerSingleton();
  }

  @Inject
  @Singleton
  @Provides
  public Tar providesTar() {
    return new TarImpl(Tar.CompressionMode.LZ4);
  }

  private static class ContentDownloaderNoTar implements Provider<ContentDownloader> {
    private static final int NUM_S3_THREADS = 20;

    @Inject AmazonS3 s3;
    @Inject LuceneServerConfiguration luceneServerConfiguration;

    public ContentDownloader get() {
      return new ContentDownloaderImpl(
          new NoTarImpl(),
          TransferManagerBuilder.standard()
              .withS3Client(s3)
              .withExecutorFactory(() -> Executors.newFixedThreadPool(NUM_S3_THREADS))
              .withShutDownThreadPools(false)
              .build(),
          luceneServerConfiguration.getBucketName(),
          true);
    }
  }

  private static class ContentDownloaderWithTar implements Provider<ContentDownloader> {
    private static final int NUM_S3_THREADS = 20;

    @Inject AmazonS3 s3;
    @Inject LuceneServerConfiguration luceneServerConfiguration;

    public ContentDownloader get() {
      return new ContentDownloaderImpl(
          new TarImpl(Tar.CompressionMode.LZ4),
          TransferManagerBuilder.standard()
              .withS3Client(s3)
              .withExecutorFactory(() -> Executors.newFixedThreadPool(NUM_S3_THREADS))
              .withShutDownThreadPools(false)
              .build(),
          luceneServerConfiguration.getBucketName(),
          true);
    }
  }

  private static class FileCompressAndUploaderNoTar implements Provider<FileCompressAndUploader> {
    private static final int NUM_S3_THREADS = 20;

    @Inject AmazonS3 s3;
    @Inject LuceneServerConfiguration luceneServerConfiguration;

    public FileCompressAndUploader get() {
      return new FileCompressAndUploader(
          new NoTarImpl(),
          TransferManagerBuilder.standard()
              .withS3Client(s3)
              .withExecutorFactory(() -> Executors.newFixedThreadPool(NUM_S3_THREADS))
              .withShutDownThreadPools(false)
              .build(),
          luceneServerConfiguration.getBucketName());
    }
  }

  private static class FileCompressAndUploaderWithTar implements Provider<FileCompressAndUploader> {
    private static final int NUM_S3_THREADS = 20;

    @Inject AmazonS3 s3;
    @Inject LuceneServerConfiguration luceneServerConfiguration;

    public FileCompressAndUploader get() {
      return new FileCompressAndUploader(
          new TarImpl(Tar.CompressionMode.LZ4),
          TransferManagerBuilder.standard()
              .withS3Client(s3)
              .withExecutorFactory(() -> Executors.newFixedThreadPool(NUM_S3_THREADS))
              .withShutDownThreadPools(false)
              .build(),
          luceneServerConfiguration.getBucketName());
    }
  }

  private static class BackupDiffManagerProvider implements Provider<BackupDiffManager> {
    @Inject
    @Named("fileCompressAndUploaderNoTar")
    FileCompressAndUploader fileCompressAndUploader;

    @Inject
    @Named("contentDownloaderNoTar")
    ContentDownloader contentDownloader;

    @Inject AmazonS3 s3;
    @Inject LuceneServerConfiguration luceneServerConfiguration;

    @Override
    public BackupDiffManager get() {
      return new BackupDiffManager(
          contentDownloader,
          fileCompressAndUploader,
          new VersionManager(s3, luceneServerConfiguration.getBucketName()),
          Paths.get(luceneServerConfiguration.getArchiveDirectory()));
    }
  }

  private static class LegacyArchiverProvider implements Provider<Archiver> {
    @Inject AmazonS3 s3;
    @Inject LuceneServerConfiguration luceneServerConfiguration;
    @Inject Tar tar;

    @Override
    public Archiver get() {
      Path archiveDir = Paths.get(luceneServerConfiguration.getArchiveDirectory());
      return new ArchiverImpl(
          s3,
          luceneServerConfiguration.getBucketName(),
          archiveDir,
          tar,
          luceneServerConfiguration.getDownloadAsStream());
    }
  }

  private static class IncArchiverProvider implements Provider<Archiver> {
    @Inject BackupDiffManager backupDiffManager;

    @Inject
    @Named("fileCompressAndUploaderWithTar")
    FileCompressAndUploader fileCompressAndUploader;

    @Inject
    @Named("contentDownloaderWithTar")
    ContentDownloader contentDownloader;

    @Inject LuceneServerConfiguration luceneServerConfiguration;
    @Inject AmazonS3 s3;

    @Override
    public Archiver get() {
      return new IndexArchiver(
          backupDiffManager,
          fileCompressAndUploader,
          contentDownloader,
          new VersionManager(s3, luceneServerConfiguration.getBucketName()),
          Paths.get(luceneServerConfiguration.getArchiveDirectory()));
    }
  }
}
