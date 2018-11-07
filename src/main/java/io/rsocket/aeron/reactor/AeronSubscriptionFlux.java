package io.rsocket.aeron.reactor;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.agrona.DirectBuffer;
import org.agrona.io.DirectBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.core.publisher.WorkQueueProcessor;

public class AeronSubscriptionFlux extends Flux<ByteBuf>
    implements org.reactivestreams.Subscription, FragmentHandler {
  static final AtomicIntegerFieldUpdater<AeronSubscriptionFlux> ONCE = AtomicIntegerFieldUpdater
      .newUpdater(AeronSubscriptionFlux.class, "once");

  static final AtomicIntegerFieldUpdater<AeronSubscriptionFlux> WIP = AtomicIntegerFieldUpdater
      .newUpdater(AeronSubscriptionFlux.class, "wip");

  static final AtomicIntegerFieldUpdater<AeronSubscriptionFlux> MISSED = AtomicIntegerFieldUpdater
      .newUpdater(AeronSubscriptionFlux.class, "missed");

  static final AtomicLongFieldUpdater<AeronSubscriptionFlux> REQUESTED = AtomicLongFieldUpdater
      .newUpdater(AeronSubscriptionFlux.class, "requested");

  private static final Logger logger = LoggerFactory.getLogger(AeronSubscriptionFlux.class);
  private static final int EFFORT = 8;
  private static final ThreadLocal<DirectBufferInputStream> DIRECT_BUFFER_INPUT_STREAM = ThreadLocal
      .withInitial(DirectBufferInputStream::new);
  private final WorkQueueProcessor<Runnable> workQueueProcessor;
  private final Subscription subscription;
  private final ByteBufAllocator allocator;
  private final FragmentAssembler assembler;
  private final String name;
  volatile int missed;
  volatile int wip;
  volatile long requested;
  volatile int once;
  volatile CoreSubscriber<? super ByteBuf> actual;
  volatile boolean cancelled;

  AeronSubscriptionFlux(String name, WorkQueueProcessor<Runnable> workQueueProcessor,
      Subscription subscription, ByteBufAllocator allocator) {
    this.name = name;
    this.workQueueProcessor = workQueueProcessor;
    this.subscription = subscription;
    this.allocator = allocator;
    this.assembler = new FragmentAssembler(this::onFragment);
  }

  public static AeronSubscriptionFlux create(String name,
      WorkQueueProcessor<Runnable> workQueueProcessor, Subscription subscription,
      ByteBufAllocator allocator) {
    return new AeronSubscriptionFlux(name, workQueueProcessor, subscription, allocator);
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
    Objects.requireNonNull(actual, "subscribe");
    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {

      actual.onSubscribe(this);
      this.actual = actual;
      if (cancelled) {
        this.actual = null;
      }
    } else {
      Operators.error(actual,
          new IllegalStateException("AeronSubscriptionFlux allows only a single Subscriber"));
    }
  }

  @Override
  public void request(long n) {
    System.out.println("n - " + n);
    if (Operators.validate(n)) {
      Operators.addCap(REQUESTED, this, n);
      tryEmit();
    }
  }

  void tryEmit() {
    MISSED.incrementAndGet(this);
    if (WIP.compareAndSet(this, 0, 1)) {
      emit();
    }
  }

  void emit() {
    for (;;) {
      MISSED.set(this, 0);

      long effort = EFFORT;
      while (effort-- > 0 && requested > 0) {
        int poll = subscription.poll(assembler, 4096);

        if (poll < 1) {
          break;
        }
      }

      if (cancelled || MISSED.get(this) == 0) {
        break;
      }
    }

    if (!cancelled && requested > 0) {
      workQueueProcessor.onNext(this::emit);
    } else {
      WIP.set(this, 0);
    }
  }

  @Override
  public void cancel() {
    System.out.printf(name + " - here");
    if (cancelled) {
      return;
    }
    cancelled = true;
    actual = null;
  }

  @Override
  public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
    try {
      ByteBuf wrappedBuffer = Unpooled.wrappedBuffer(buffer.byteBuffer());
      ByteBuf byteBuf = allocator.buffer(length);
      byteBuf.writeBytes(wrappedBuffer, offset, length);

      if (logger.isDebugEnabled()) {
        logger.debug(name + " receiving:\n{}\n", ByteBufUtil.prettyHexDump(byteBuf));
      }

//      System.out.println("actual " + toString());
      actual.onNext(byteBuf);

      Operators.addCap(REQUESTED, this, -1);
    } catch (Throwable t) {
      logger.error("error receiving bytes", t);
      actual.onError(t);
    }
  }
}
