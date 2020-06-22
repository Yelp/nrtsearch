package com.yelp.nrtsearch.server.grpc;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.stub.ServerCallStreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;

// Set up a back-pressure-aware consumer for the request stream. The onReadyHandler will be invoked
// when the consuming side has enough buffer space to receive more messages.
//
// Note: the onReadyHandler's invocation is serialized on the same thread pool as the incoming StreamObserver's
// onNext(), onError(), and onComplete() handlers. Blocking the onReadyHandler will prevent additional messages
// from being processed by the incoming StreamObserver. The onReadyHandler must return in a timely manner or
// else message processing throughput will suffer.
public class OnReadyHandler implements Runnable {
    private final ServerCallStreamObserver<? extends GeneratedMessageV3> serverCallStreamObserver;

    // Guard against spurious onReady() calls caused by a race between onNext() and onReady(). If the transport
    // toggles isReady() from false to true while onNext() is executing, but before onNext() checks isReady(),
    // request(1) would be called twice - once by onNext() and once by the onReady() scheduled during onNext()'s
    // execution.
    public AtomicBoolean wasReady = new AtomicBoolean(false);

    public OnReadyHandler(ServerCallStreamObserver<? extends GeneratedMessageV3> serverCallStreamObserver) {
        this.serverCallStreamObserver = serverCallStreamObserver;
    }

    @Override
    public void run() {
        if (serverCallStreamObserver.isReady() && !wasReady.get()) {
            wasReady.set(true);
            // Signal the request sender to send one message. This happens when isReady() turns true, signaling that
            // the receive buffer has enough free space to receive more messages. Calling request() serves to prime
            // the message pump.
            serverCallStreamObserver.request(1);
        }
    }
}