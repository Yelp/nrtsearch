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
package com.yelp.nrtsearch.server.cli;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.yelp.nrtsearch.server.Version;
import com.yelp.nrtsearch.server.backup.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "backup-restore",
    synopsisSubcommandLabel = "COMMAND",
    subcommands = {BackupHelper.class, RestoreHelper.class})
public class BackupRestoreCommand implements Callable<Integer> {
  private static final Logger logger =
      LoggerFactory.getLogger(BackupRestoreCommand.class.getName());

  @CommandLine.Option(
      names = {"-s", "--serviceName"},
      description = "Name of the service which is to be backed up",
      required = true)
  private String serviceName;

  public String getServiceName() {
    return serviceName;
  }

  @CommandLine.Option(
      names = {"-r", "--resourceName"},
      description = "Name of the resource which is to be backed up",
      required = true)
  private String resourceName;

  public String getResourceName() {
    return resourceName;
  }

  @CommandLine.Option(
      names = {"-c", "--botoCfg"},
      description = "Path to botoCfg file on disk",
      required = true)
  private String botoCfg;

  public String getBotoCfgPath() {
    return botoCfg;
  }

  @CommandLine.Option(
      names = {"-b", "--bucketName"},
      description = "S3 bucket Name",
      required = true)
  private String bucket;

  public String getBucket() {
    return bucket;
  }

  @CommandLine.Option(
      names = {"-a", "--dir"},
      description = "Path to archiver directory on disk",
      required = true)
  private String dir;

  public String getArchiveDir() {
    return dir;
  }

  @CommandLine.Option(
      names = {"-t", "--tar"},
      description = "Tar the backup/restore")
  private boolean tar;

  public boolean isTar() {
    return tar;
  }

  @CommandLine.Option(
      names = {"-V", "--version"},
      description = "Print version information and exit")
  private boolean printVersion;

  public static void main(String... args) {
    int exitCode = new CommandLine(new BackupRestoreCommand()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    if (printVersion) {
      System.out.println(Version.CURRENT);
    } else {
      // if only the base command is run, just print the usage
      new CommandLine(this).execute("help");
    }
    return 0;
  }

  public Archiver getArchiver() {
    AmazonS3 s3 = providesAmazonS3(this);
    TransferManager transferManager =
        TransferManagerBuilder.standard().withS3Client(s3).withShutDownThreadPools(false).build();
    Tar currentTar = this.isTar() ? new TarImpl(TarImpl.CompressionMode.LZ4) : new NoTarImpl();
    ContentDownloaderImpl contentDownloader =
        new ContentDownloaderImpl(currentTar, transferManager, this.getBucket(), true);
    FileCompressAndUploader fileCompressAndUploader =
        new FileCompressAndUploader(currentTar, transferManager, this.getBucket());
    VersionManager versionManager = new VersionManager(s3, this.getBucket());

    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            contentDownloader,
            fileCompressAndUploader,
            versionManager,
            Path.of(this.getArchiveDir()));
    ContentDownloaderImpl contentDownloaderTar =
        new ContentDownloaderImpl(
            new TarImpl(TarImpl.CompressionMode.LZ4), transferManager, this.getBucket(), true);
    FileCompressAndUploader fileCompressAndUploaderTar =
        new FileCompressAndUploader(
            new TarImpl(TarImpl.CompressionMode.LZ4), transferManager, this.getBucket());
    return new IndexArchiver(
        backupDiffManager,
        fileCompressAndUploaderTar,
        contentDownloaderTar,
        versionManager,
        Path.of(this.getArchiveDir()));
  }

  public AmazonS3 getAmazonS3() {
    return providesAmazonS3(this);
  }

  private static AmazonS3 providesAmazonS3(BackupRestoreCommand backupRestoreCommand) {
    {
      Path botoCfgPath = Paths.get(backupRestoreCommand.getBotoCfgPath());
      final ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile(botoCfgPath.toFile());
      final AWSCredentialsProvider awsCredentialsProvider =
          new ProfileCredentialsProvider(profilesConfigFile, "default");
      AmazonS3 s3ClientInterim =
          AmazonS3ClientBuilder.standard().withCredentials(awsCredentialsProvider).build();
      String region = s3ClientInterim.getBucketLocation(backupRestoreCommand.getBucket());
      // In useast-1, the region is returned as "US" which is an equivalent to "us-east-1"
      // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/Region.html#US_Standard
      // However, this causes an UnknownHostException so we override it to the full region name
      if (region.equals("US")) {
        region = "us-east-1";
      }
      String serviceEndpoint = String.format("s3.%s.amazonaws.com", region);
      logger.info(String.format("S3 ServiceEndpoint: %s", serviceEndpoint));
      return AmazonS3ClientBuilder.standard()
          .withCredentials(awsCredentialsProvider)
          .withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
          .build();
    }
  }
}
