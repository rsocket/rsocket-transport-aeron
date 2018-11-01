package io.rsocket.aeron;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.rsocket.aeron.reactor.AeronPublicationSubscriber;
import io.rsocket.aeron.reactor.AeronSubscriptionFlux;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.core.publisher.WorkQueueProcessor;

public class AeronPublicationSubscriberTest {
  private final int count = 10_000_000;

  @Test
  public void testSubscriber() throws Exception {
    MediaDriverHolder.getInstance();

    Aeron aeron = Aeron.connect();
    ConcurrentPublication publication = aeron.addPublication("aeron:ipc", 1);
    Subscription subscription = aeron.addSubscription("aeron:ipc", 1);
    WorkQueueProcessor<Runnable> processor = WorkQueueProcessor.create();
    processor.doOnNext(Runnable::run).subscribe();

    Function<? super Publisher<ByteBuf>, ? extends Publisher<Object>> lift = Operators
        .lift((scannable, coreSubscriber) -> AeronPublicationSubscriber.create("test", processor,
            coreSubscriber, publication));

    CountDownLatch latch = new CountDownLatch(count);
    ForkJoinPool.commonPool().execute(() -> {
      int retries = 3;
      while (retries != 0) {
        int poll = subscription.poll(new FragmentAssembler((buffer, offset, length, header) -> {
          long count1 = latch.getCount();
          if (count1 % 10_000 == 0) {
            System.out.println("here " + count1);
          }
          latch.countDown();
        }), 4096);
        if (poll == 0) {
          retries--;
          try {
            TimeUnit.MILLISECONDS.sleep(100);
          } catch (InterruptedException ignoredException) {
            break;
          }
        } else if (latch.getCount() > 0){
          retries = 3;
        } else {
          break;
        }
      }
    });

    Flux.range(1, count).map(i -> Unpooled.buffer().writeInt(i)).transform(lift).blockLast();
    Assert.assertTrue(latch.await(5, TimeUnit.MINUTES));
  }

  @Test
  public void testFlux() throws Exception {
    MediaDriverHolder.getInstance();

    Aeron aeron = Aeron.connect();
    Publication publication = aeron.addExclusivePublication("aeron:ipc", 1);
    Subscription subscription = aeron.addSubscription("aeron:ipc", 1);

    WorkQueueProcessor<Runnable> processor = WorkQueueProcessor.create();
    processor.doOnNext(Runnable::run).subscribe();

    Function<? super Publisher<ByteBuf>, ? extends Publisher<Object>> lift = Operators
        .lift((scannable, coreSubscriber) -> AeronPublicationSubscriber.create("test", processor,
            coreSubscriber, publication));

    CountDownLatch latch = new CountDownLatch(count);

    AeronSubscriptionFlux.create("test", processor, subscription, ByteBufAllocator.DEFAULT)
        .doOnNext(b -> {
          long count1 = latch.getCount();
          if (count1 == 0) {
            System.out.println("Done");
          }
          else if (count1 % 100_000 == 0) {
            System.out.println(Thread.currentThread().getName() + " - here " + count1);
          }
          System.out.println(ByteBufUtil.prettyHexDump(b));

          latch.countDown();
          b.release();
        }).subscribe();

    Flux.range(1, count).map(i -> Unpooled.buffer().writeInt(i)).transform(lift).blockLast();
    Assert.assertTrue(latch.await(5, TimeUnit.MINUTES));
  }
}
