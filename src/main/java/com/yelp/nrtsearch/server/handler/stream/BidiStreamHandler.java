/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.handler.stream;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for a bidirectional streaming RPC whose sessions hold server resources between
 * messages, such as a Lucene searcher pinning an index commit.
 *
 * <p>Deliberately not a {@link com.yelp.nrtsearch.server.handler.Handler}: that base models a
 * request/response call, where the handler produces one response and owns nothing afterwards. A
 * stream instead needs a per-call session, and the two things a long-lived session must not be
 * trusted to do on its own are bounding how many exist and bounding how long an idle one survives.
 * Both live here so that a new streaming RPC gets them by construction:
 *
 * <ul>
 *   <li>a semaphore admitting at most {@code maxConcurrentStreams} sessions, returned on every
 *       terminal path;
 *   <li>an idle timeout armed when the stream opens and re-armed after each response that expects
 *       another message, so a client that goes silent cannot hold resources indefinitely.
 * </ul>
 *
 * <p>What a stream carries from one message to the next lives in an {@link io.grpc.Context} rather
 * than in fields of the {@link StreamObserver}, and subclasses reach it through {@link
 * Session#sessionState()}. See {@link Session} for how that context is scoped.
 *
 * @param <T> request message type
 * @param <S> response message type
 * @param <C> type holding what one stream carries between messages
 */
public abstract class BidiStreamHandler<T, S, C> {
  private static final Logger logger = LoggerFactory.getLogger(BidiStreamHandler.class);

  private final String streamName;
  private final int maxConcurrentStreams;
  protected final long idleTimeoutMs;
  private final Semaphore concurrencyLimiter;
  private final ScheduledExecutorService timeoutScheduler;
  private final Context.Key<C> sessionStateKey;

  /**
   * @param streamName name used in log lines and client-facing timeout messages
   * @param maxConcurrentStreams maximum number of sessions that may be open at once
   * @param idleTimeoutMs how long a session may wait on the client before being closed
   * @param timeoutThreads size of the idle timeout scheduler pool
   */
  protected BidiStreamHandler(
      String streamName, int maxConcurrentStreams, long idleTimeoutMs, int timeoutThreads) {
    this.streamName = streamName;
    this.maxConcurrentStreams = maxConcurrentStreams;
    this.idleTimeoutMs = idleTimeoutMs;
    this.concurrencyLimiter = new Semaphore(maxConcurrentStreams);
    this.sessionStateKey = Context.key(streamName + "-session-state");
    AtomicInteger threadId = new AtomicInteger();
    // Idle timeouts only need to release resources and write a status, so a small pool is enough. A
    // slow release can still delay other sessions' timeouts, which delays cleanup but does not
    // affect any in flight request.
    this.timeoutScheduler =
        Executors.newScheduledThreadPool(
            timeoutThreads,
            r -> {
              Thread t = new Thread(r, streamName + "-timeout-" + threadId.getAndIncrement());
              t.setDaemon(true);
              return t;
            });
  }

  /**
   * Admit a stream and return the observer that will receive its messages. Rejects with {@code
   * RESOURCE_EXHAUSTED} once {@code maxConcurrentStreams} sessions are open.
   */
  public final StreamObserver<T> handle(StreamObserver<S> responseObserver) {
    if (!concurrencyLimiter.tryAcquire()) {
      responseObserver.onError(
          Status.RESOURCE_EXHAUSTED
              .withDescription(
                  "Maximum concurrent "
                      + streamName
                      + " streams exceeded ("
                      + maxConcurrentStreams
                      + ")")
              .asRuntimeException());
      return new NoOpStreamObserver<>();
    }
    Session session = newSession(responseObserver);
    // Arm the idle timer as soon as the permit is taken, so that a client which opens a stream and
    // then goes silent cannot hold the permit forever. Safe to do here since grpc cannot deliver a
    // message until this observer is returned.
    try {
      session.scheduleIdleTimeout();
    } catch (RejectedExecutionException e) {
      // The scheduler is shut down, so this stream would have no idle timeout at all. Hand the
      // permit back rather than leaking it, since no further callback will run for this session.
      // Run the release inside the session context like every other path, since a subclass is
      // entitled to read its session state while tearing down.
      session.inSessionContext(session::releaseResources);
      responseObserver.onError(
          Status.UNAVAILABLE.withDescription("Server is shutting down").asRuntimeException());
      return new NoOpStreamObserver<>();
    }
    return session;
  }

  /** Create the session for one stream. Called once per admitted stream. */
  protected abstract Session newSession(StreamObserver<S> responseObserver);

  /**
   * Create the state one stream carries between messages. Called once per session, from the {@link
   * Session} constructor, and reachable from there on through {@link Session#sessionState()}.
   */
  protected abstract C newSessionState();

  /**
   * Number of streams currently holding a permit. Test-only for now: tests assert that permits are
   * handed back on every terminal path, which is not observable from the responses. It is also the
   * natural source for an active-sessions gauge when streaming metrics land.
   */
  @VisibleForTesting
  int getActiveStreams() {
    return maxConcurrentStreams - concurrencyLimiter.availablePermits();
  }

  /**
   * Stop the idle timeout scheduler. Any open sessions are left to be closed by grpc.
   *
   * <p>Must run <em>after</em> {@code Server.awaitTermination()}: graceful gRPC shutdown waits for
   * in-flight RPCs, and a stream of this kind is only guaranteed to end because of its idle
   * timeout, so stopping the scheduler first would hang shutdown on a stalled stream.
   */
  public void shutdown() {
    timeoutScheduler.shutdownNow();
  }

  /**
   * One stream's session. Owns the concurrency permit, the idle timer, and the {@link
   * io.grpc.Context} that carries this stream's state.
   *
   * <p>That context is derived from the call's context when the stream is admitted, so it inherits
   * the call's deadline and cancellation, and it is attached around every callback and around the
   * idle timeout task. Deriving it here rather than in a {@link io.grpc.ServerInterceptor} is what
   * lets the timeout task read the state at all: a context propagates by thread local, and that
   * task runs on a scheduler thread rather than a gRPC one, so the context has to be applied
   * explicitly. It also means a handler driven directly, outside a gRPC call, behaves the same as
   * one driven through the server.
   *
   * <p>Every method that touches session state synchronizes on the session, including the idle
   * timeout task, so a timeout can never interleave with a message being handled.
   */
  protected abstract class Session implements StreamObserver<T> {
    protected final StreamObserver<S> responseObserver;
    private final Context sessionContext;
    private final AtomicReference<ScheduledFuture<?>> idleTimer = new AtomicReference<>();
    protected boolean closed = false;

    protected Session(StreamObserver<S> responseObserver) {
      this.responseObserver = responseObserver;
      this.sessionContext = Context.current().withValue(sessionStateKey, newSessionState());
    }

    /** What this stream carries between messages, from the session's {@link io.grpc.Context}. */
    protected final C sessionState() {
      C state = sessionStateKey.get();
      if (state == null) {
        // Only reachable if a callback body runs outside the session context, which would otherwise
        // look like a stream that inexplicably forgot its own search.
        throw Status.INTERNAL
            .withDescription(streamName + " session state is not attached to the current context")
            .asRuntimeException();
      }
      return state;
    }

    @Override
    public final void onNext(T request) {
      inSessionContext(() -> handleNext(request));
    }

    @Override
    public final void onError(Throwable t) {
      inSessionContext(() -> handleError(t));
    }

    @Override
    public final void onCompleted() {
      inSessionContext(this::handleCompleted);
    }

    /** Handle one request message, with this session's state attached. */
    protected abstract void handleNext(T request);

    /** Handle client error or cancellation, with this session's state attached. */
    protected abstract void handleError(Throwable t);

    /** Handle the client half-closing the stream, with this session's state attached. */
    protected abstract void handleCompleted();

    private void inSessionContext(Runnable body) {
      Context previous = sessionContext.attach();
      try {
        body.run();
      } finally {
        sessionContext.detach(previous);
      }
    }

    /**
     * Arm the idle timeout. Called once when the stream opens and again after every response that
     * expects a further message from the client.
     */
    protected final synchronized void scheduleIdleTimeout() {
      IdleTimeoutTask task = new IdleTimeoutTask();
      // Assigning the field after scheduling is safe because every caller holds the session
      // monitor, and the task body must acquire that monitor before it reads the field.
      task.future =
          timeoutScheduler.schedule(
              sessionContext.wrap(task), idleTimeoutMs, TimeUnit.MILLISECONDS);
      idleTimer.set(task.future);
    }

    protected final void cancelIdleTimeout() {
      // Clearing the reference is what invalidates a task that has already started running and is
      // waiting on the monitor; the cancel only saves the wakeup.
      ScheduledFuture<?> future = idleTimer.getAndSet(null);
      if (future != null) {
        future.cancel(false);
      }
    }

    /**
     * Release everything this session holds, and hand back the concurrency permit. Must be
     * idempotent, and must set {@link #closed}; subclasses override to add their own resources and
     * call {@link #releasePermit()} last.
     */
    protected abstract void releaseResources();

    /** Hand back the concurrency permit. Call exactly once, from {@link #releaseResources()}. */
    protected final void releasePermit() {
      concurrencyLimiter.release();
    }

    /**
     * Release resources and then fail the stream. Releasing first means that the moment a client
     * observes the end of this stream the permit is already back, so a client that immediately
     * retries cannot be rejected by a session that is already finished.
     */
    protected final void closeWithError(Throwable error) {
      if (closed) {
        return;
      }
      releaseResources();
      try {
        responseObserver.onError(error);
      } catch (Exception e) {
        logger.debug("Failed to send error to {} client", streamName, e);
      }
    }

    /** Set response compression on this stream, falling back to uncompressed on any error. */
    protected final void setResponseCompression(String compressionType) {
      if (!compressionType.isEmpty()) {
        try {
          ((ServerCallStreamObserver<?>) responseObserver).setCompression(compressionType);
        } catch (Exception e) {
          logger.warn("Unable to set response compression to type '{}'", compressionType, e);
        }
      }
    }

    /**
     * Closes the session once it has been idle for {@code idleTimeoutMs}, unless it is no longer
     * the current timer.
     *
     * <p>The task compares {@link #idleTimer} against its own future because {@link
     * ScheduledFuture#cancel(boolean)} cannot stop a task that has already started, and this one
     * starts by blocking on the session monitor that {@code onNext} holds. Without the check, a
     * timeout that fired while {@code onNext} was producing a perfectly good response would acquire
     * the monitor afterwards and close the stream, so the client would see a valid response
     * followed by a spurious DEADLINE_EXCEEDED.
     */
    private final class IdleTimeoutTask implements Runnable {
      private ScheduledFuture<?> future;

      @Override
      public void run() {
        synchronized (Session.this) {
          if (closed || idleTimer.get() != future) {
            return;
          }
          logger.info("{} idle timeout reached, closing session", streamName);
          closeWithError(
              Status.DEADLINE_EXCEEDED
                  .withDescription(streamName + " idle for more than " + idleTimeoutMs + "ms")
                  .asRuntimeException());
        }
      }
    }
  }

  /** Discards everything, for a stream that was rejected before a session was created. */
  private static class NoOpStreamObserver<R> implements StreamObserver<R> {
    @Override
    public void onNext(R value) {}

    @Override
    public void onError(Throwable t) {}

    @Override
    public void onCompleted() {}
  }
}
