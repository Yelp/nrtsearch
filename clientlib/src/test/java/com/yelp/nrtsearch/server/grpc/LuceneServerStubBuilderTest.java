package com.yelp.nrtsearch.server.grpc;

import org.junit.Test;

public class LuceneServerStubBuilderTest {
    public static final String HOST = "host";
    public static final int PORT = 9999;

    @Test public void testChannelCreation() {
        LuceneServerStubBuilder stubBuilder = new LuceneServerStubBuilder(HOST, PORT);
        assert stubBuilder.channel.authority().equals(String.format("%s:%d", HOST, PORT));
    }

    @Test public void testBlockingStubCreation() {
        LuceneServerStubBuilder stubBuilder = new LuceneServerStubBuilder(HOST, PORT);
        assert stubBuilder.createBlockingStub().getChannel().authority().equals(
            String.format("%s:%d", HOST, PORT));
    }

    @Test public void testAsyncStubCreation() {
        LuceneServerStubBuilder stubBuilder = new LuceneServerStubBuilder(HOST, PORT);
        assert stubBuilder.createAsyncStub().getChannel().authority().equals(
            String.format("%s:%d", HOST, PORT));
    }

    @Test public void testFutureStubCreation() {
        LuceneServerStubBuilder stubBuilder = new LuceneServerStubBuilder(HOST, PORT);
        assert stubBuilder.createFutureStub().getChannel().authority().equals(
            String.format("%s:%d", HOST, PORT));
    }
}
