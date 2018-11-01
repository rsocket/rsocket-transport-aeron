package io.rsocket.aeron;

import io.aeron.Aeron;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.aeron.client.AeronClientTransport;
import io.rsocket.aeron.server.AeronServerTransport;
import io.rsocket.util.DefaultPayload;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.WorkQueueProcessor;

public class AeronTest {
  public static final String aeronUrl = "aeron:udp?endpoint=127.0.0.1:39790";

  @Test
  public void test() throws Exception {

    MediaDriverHolder.getInstance();
    Aeron aeron = Aeron.connect();

    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    WorkQueueProcessor<Runnable> workQueueProcessor = WorkQueueProcessor.create();
    workQueueProcessor.doOnNext(Runnable::run).doOnError(Throwable::printStackTrace)
        .onErrorResume(t -> Mono.empty()).subscribe();

    AeronClientTransport client;
    client = new AeronClientTransport(workQueueProcessor, aeron, aeronUrl, allocator);

    CountDownLatch latch = new CountDownLatch(1);
    Executors.newSingleThreadExecutor().execute(() -> {
      WorkQueueProcessor<Runnable> workQueueProcessor1 = WorkQueueProcessor.create();
      workQueueProcessor1.doOnNext(Runnable::run).doOnError(Throwable::printStackTrace)
          .onErrorResume(t -> Mono.empty()).subscribe();

      AeronServerTransport server;

      server = new AeronServerTransport(workQueueProcessor1, aeron, aeronUrl, allocator);
      RSocketFactory.receive().acceptor((setup, sendingSocket) -> Mono.just(new AbstractRSocket() {
        @Override
        public Mono<Payload> requestResponse(Payload payload) {
          System.out.println("got from client ->" + payload.getDataUtf8());
          return Mono.just(DefaultPayload.create("server sending response"));
        }
      })).transport(server).start().block();

      latch.countDown();
    });

    latch.await();

    RSocket block = RSocketFactory.connect().transport(client).start().block();

    Payload ahoy = block.requestResponse(DefaultPayload.create("client sending request")).block();

    System.out.println("got from server -> " + ahoy.getDataUtf8());
  }
}
