package io.rsocket.aeron.reactor;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.aeron.util.Constants;
import io.rsocket.aeron.util.NotConnectedException;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Operators;
import reactor.core.publisher.WorkQueueProcessor;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class AeronPublicationSubscriber extends AtomicBoolean
    implements Subscription, CoreSubscriber<ByteBuf> {
  private static final Logger logger = LoggerFactory.getLogger(AeronPublicationSubscriber.class);
  private static final ThreadLocal<BufferClaim> BUFFER_CLAIMS =
      ThreadLocal.withInitial(BufferClaim::new);
  private static final ThreadLocal<UnsafeBuffer> UNSAFE_BUFFER =
      ThreadLocal.withInitial(UnsafeBuffer::new);

  private static final int BUFFER_SIZE = 128;
  private static final int REFILL = BUFFER_SIZE / 3;

  private static final int MAX_EFFORT = 8;
  private final CoreSubscriber<? super ByteBuf> actual;
  private final Publication publication;
  private final ManyToOneConcurrentArrayQueue<ByteBuf> frameQueue;
  private final WorkQueueProcessor<Runnable> workQueueProcessor;
  private volatile ByteBuf currentWorkingFrame;
  private Subscription subscription;

  private AtomicIntegerFieldUpdater<AeronPublicationSubscriber> MISSED =
      AtomicIntegerFieldUpdater.newUpdater(AeronPublicationSubscriber.class, "missed");
  private volatile int missed;
  private AtomicIntegerFieldUpdater<AeronPublicationSubscriber> WIP =
      AtomicIntegerFieldUpdater.newUpdater(AeronPublicationSubscriber.class, "wip");
  private volatile int wip;
  private AtomicLongFieldUpdater<AeronPublicationSubscriber> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(AeronPublicationSubscriber.class, "requested");
  private volatile long requested;

  private AtomicLongFieldUpdater<AeronPublicationSubscriber> REQUESTED_UPSTREAM =
      AtomicLongFieldUpdater.newUpdater(AeronPublicationSubscriber.class, "requestedUpstream");
  private volatile long requestedUpstream;

  private final String name;
  
  AeronPublicationSubscriber(
      String name,
      WorkQueueProcessor<Runnable> workQueueProcessor,
      CoreSubscriber<? super ByteBuf> actual,
      Publication publication) {
    this.name = name;
    this.workQueueProcessor = workQueueProcessor;
    this.actual = actual;
    this.publication = publication;
    this.frameQueue = new ManyToOneConcurrentArrayQueue<>(BUFFER_SIZE);
  }

  public static AeronPublicationSubscriber create(
      String name,
      WorkQueueProcessor<Runnable> workQueueProcessor,
      CoreSubscriber<? super ByteBuf> actual,
      Publication publication) {
    return new AeronPublicationSubscriber(name, workQueueProcessor, actual, publication);
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (Operators.validate(this.subscription, s)) {
      this.subscription = s;

      actual.onSubscribe(this);
      requestedUpstream = BUFFER_SIZE;
      subscription.request(BUFFER_SIZE);
    }
  }

  @Override
  public void request(long n) {
    Operators.addCap(REQUESTED, this, n);
    tryEmit();
  }

  @Override
  public void cancel() {
    if (!frameQueue.isEmpty()) {
      frameQueue.drain(ReferenceCountUtil::safeRelease);
    }
  }

  @Override
  public void onNext(ByteBuf byteBuf) {
    if (!frameQueue.offer(byteBuf)) {
      throw new IllegalStateException("missing back pressure ");
    }

    tryEmit();
  }

  @Override
  public void onError(Throwable t) {
    t.printStackTrace();
    if (subscription != null) {
      subscription.cancel();
    }
    actual.onError(t);
  }

  @Override
  public void onComplete() {
    set(true);
    tryEmit();
  }

  private void tryEmit() {
    MISSED.incrementAndGet(this);

    if (WIP.compareAndSet(this, 0, 1)) {
      emit();
    }
  }

  private void emit() {
    for (; ; ) {
      MISSED.set(this, 0);

      while (!frameQueue.isEmpty()) {
        if (currentWorkingFrame == null) {
          currentWorkingFrame = frameQueue.poll();
        }

        if (currentWorkingFrame != null && REQUESTED.get(this) > 0) {
          if (tryClaimOrOffer(currentWorkingFrame)) {
            Operators.addCap(REQUESTED, this, -1);
            long l = Operators.addCap(REQUESTED_UPSTREAM, this, -1);
  
            if (logger.isDebugEnabled()) {
              logger.debug(name + " emitted frame: \n{}\n", ByteBufUtil.prettyHexDump(currentWorkingFrame));
            }
  
            currentWorkingFrame.release();
            currentWorkingFrame = null;
            if (l < REFILL) {
              int r = BUFFER_SIZE - frameQueue.size();
              Operators.addCap(REQUESTED_UPSTREAM, this, r);
              subscription.request(r);
            }
          } else {
            break;
          }
        } else {
          break;
        }
      }

      if (MISSED.get(this) == 0) {
        break;
      }
    }

    if (currentWorkingFrame != null) {
      workQueueProcessor.onNext(this::emit);
    } else {
      if (get() && frameQueue.isEmpty()) {
        actual.onComplete();
      }

      WIP.set(this, 0);
    }
  }

  private boolean tryClaimOrOffer(ByteBuf content) {
    ByteBuffer byteBuffer = content.nioBuffer();
    boolean successful = false;
    int offset = content.readerIndex();
    int capacity = content.readableBytes();

    int effort = MAX_EFFORT;
    if (capacity < Constants.AERON_MTU_SIZE) {
      while (!successful && effort-- > 0) {
        BufferClaim bufferClaim = BUFFER_CLAIMS.get();
        long offer = publication.tryClaim(capacity, bufferClaim);
        if (offer >= 0) {
          try {
            final MutableDirectBuffer b = bufferClaim.buffer();
            int claimOffset = bufferClaim.offset();
            b.putBytes(claimOffset, byteBuffer, offset, capacity);
          } finally {
            bufferClaim.commit();
            successful = true;
          }
        } else {
          if (offer == Publication.CLOSED) {
            onError(new NotConnectedException());
            break;
          }
        }
      }
    } else {
      while (!successful && effort-- > 0) {
        UnsafeBuffer unsafeBuffer = UNSAFE_BUFFER.get();
        unsafeBuffer.wrap(byteBuffer, offset, capacity);
        long offer = publication.offer(unsafeBuffer);
        if (offer < 0) {
          if (offer == Publication.CLOSED) {
            onError(new NotConnectedException());
            break;
          }
        } else {
          successful = true;
        }
      }
    }

    return successful;
  }
}
