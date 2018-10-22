package io.rsocket.aeron;

import io.aeron.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.aeron.reactor.AeronPublicationSubscriber;
import io.rsocket.aeron.reactor.AeronSubscriptionFlux;
import org.agrona.DirectBuffer;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.core.publisher.WorkQueueProcessor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

public class AeronPublicationSubscriberTest {
  @Test
  public void testSubscriber() throws Exception {
    int count = 10_000_000;
    MediaDriverHolder.getInstance();

    Aeron aeron = Aeron.connect();
    ConcurrentPublication publication = aeron.addPublication("aeron:ipc", 1);
    Subscription subscription = aeron.addSubscription("aeron:ipc", 1);
    WorkQueueProcessor<Runnable> processor = WorkQueueProcessor.create();
    processor.doOnNext(Runnable::run).subscribe();

    Function<? super Publisher<ByteBuf>, ? extends Publisher<Object>> lift =
        Operators.lift(
            (scannable, coreSubscriber) ->
                AeronPublicationSubscriber.create("test", processor, coreSubscriber, publication));

    CountDownLatch latch = new CountDownLatch(count);
    ForkJoinPool.commonPool()
        .execute(
            () -> {
              while (true) {
                subscription.poll(
                    new FragmentAssembler(
                        new FragmentHandler() {
                          @Override
                          public void onFragment(
                              DirectBuffer buffer, int offset, int length, Header header) {
                            long count1 = latch.getCount();
                            if (count1 % 10_000 == 0) {
                              System.out.println("here " + count1);
                            }
                            latch.countDown();
                          }
                        }),
                    4096);
              }
            });

    Flux.range(1, count).map(i -> Unpooled.buffer().writeInt(i)).transform(lift).blockLast();
    latch.await();
  }

  @Test
  public void testFlux() throws Exception {
    int count = 10_000_000;
    MediaDriverHolder.getInstance();

    Aeron aeron = Aeron.connect();
    Publication publication = aeron.addExclusivePublication("aeron:ipc", 1);
    Subscription subscription = aeron.addSubscription("aeron:ipc", 1);

    WorkQueueProcessor<Runnable> processor = WorkQueueProcessor.create();
    processor.doOnNext(Runnable::run).subscribe();

    Function<? super Publisher<ByteBuf>, ? extends Publisher<Object>> lift =
        Operators.lift(
            (scannable, coreSubscriber) ->
                AeronPublicationSubscriber.create("test", processor, coreSubscriber, publication));

    CountDownLatch latch = new CountDownLatch(count);

    AeronSubscriptionFlux.create("test", processor, subscription, ByteBufAllocator.DEFAULT)
        .doOnNext(
            b -> {
              long count1 = latch.getCount();
              if (count1 % 100_000 == 0) {
                System.out.println(Thread.currentThread().getName() + " - here " + count1);
              }
              latch.countDown();
              b.release();
            })
        .subscribe();

    Flux.range(1, count).map(i -> Unpooled.buffer().writeInt(i)).transform(lift).blockLast();
    latch.await();
  }
}
