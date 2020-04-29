package com.yelp.nrtsearch.server.config;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class ThreadPoolConfigurationTest {

    @Test
    public void testConfiguration() throws FileNotFoundException {
        String config = Paths.get("src", "test", "resources", "config.yaml").toAbsolutePath().toString();
        LuceneServerConfiguration luceneServerConfiguration = new LuceneServerConfiguration(new FileInputStream(config));
        assertEquals("lucene_server_foo", luceneServerConfiguration.getNodeName());
        assertEquals("foohost", luceneServerConfiguration.getHostName());
        assertEquals(luceneServerConfiguration.getThreadPoolConfiguration().getMaxSearchingThreads(), 16);
        assertEquals(luceneServerConfiguration.getThreadPoolConfiguration().getMaxSearchBufferedItems(), 100);
    }
}