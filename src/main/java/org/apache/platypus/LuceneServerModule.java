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

package org.apache.platypus;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.platypus.server.config.LuceneServerConfiguration;
import org.apache.platypus.server.utils.Archiver;
import org.apache.platypus.server.utils.ArchiverImpl;
import org.apache.platypus.server.utils.Tar;
import org.apache.platypus.server.utils.TarImpl;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.platypus.server.config.LuceneServerConfiguration.DEFAULT_BOTO_CFG_PATH;

public class LuceneServerModule extends AbstractModule {
    private final String[] args;

    public LuceneServerModule(String[] args) {
        this.args = args;
    }

    @Inject
    @Singleton
    @Provides
    public Tar providesTar() {
        return new TarImpl(Tar.CompressionMode.LZ4);
    }

    @Inject
    @Singleton
    @Provides
    protected AmazonS3 providesAmazonS3(LuceneServerConfiguration luceneServerConfiguration) {
        if (luceneServerConfiguration.getBotoCfgPath().equals(DEFAULT_BOTO_CFG_PATH.toString())) {
            return AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("dummyService", "dummyRegion"))
                    .build();
        } else {
            Path botoCfgPath = Paths.get(luceneServerConfiguration.getBotoCfgPath());
            final ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile(botoCfgPath.toFile());
            final AWSCredentialsProvider awsCredentialsProvider = new ProfileCredentialsProvider(profilesConfigFile, "default");
            AmazonS3 s3ClientInterim = AmazonS3ClientBuilder.standard()
                    .withCredentials(awsCredentialsProvider).build();
            String region = s3ClientInterim.getBucketLocation(luceneServerConfiguration.getBucketName());
            String serviceEndpoint = String.format("s3.%s.amazonaws.com", region);
            return AmazonS3ClientBuilder.standard()
                    .withCredentials(awsCredentialsProvider)
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
                    .build();
        }
    }

    @Inject
    @Singleton
    @Provides
    protected Archiver providesArchiver(LuceneServerConfiguration luceneServerConfiguration, AmazonS3 amazonS3, Tar tar) {
        Path archiveDir = Paths.get(luceneServerConfiguration.getArchiveDirectory());
        return new ArchiverImpl(amazonS3, luceneServerConfiguration.getBucketName(), archiveDir, tar);
    }

    @Inject
    @Singleton
    @Provides
    protected LuceneServerConfiguration providesLuceneServerConfiguration() throws FileNotFoundException {
        LuceneServerConfiguration luceneServerConfiguration;
        if (args.length == 0) {
            Path filePath = Paths.get("src", "main", "resources", "lucene_server_default_configuration.yaml");
            luceneServerConfiguration = new Yaml().load(new FileInputStream(filePath.toFile()));
        } else {
            luceneServerConfiguration = new Yaml().load(new FileInputStream(args[0]));
        }
        return luceneServerConfiguration;
    }

}

