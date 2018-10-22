package io.rsocket.aeron;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.aeron.reactor.AeronPublicationSubscriber;
import io.rsocket.aeron.reactor.AeronSubscriptionFlux;
import org.reactivestreams.Publisher;
import reactor.core.publisher.*;

import java.util.function.Function;

public class AeronDuplexConnection implements DuplexConnection {
  private final String name;
  private final Function<? super Publisher<ByteBuf>, ? extends Publisher<ByteBuf>> lift;
  private final AeronSubscriptionFlux receiveFlux;
  private final MonoProcessor<Void> onClose;

  public AeronDuplexConnection(
      String name,
      WorkQueueProcessor<Runnable> workQueueProcessor,
      Publication publication,
      Subscription subscription,
      ByteBufAllocator allocator) {
    this.name = name;
    this.lift =
        Operators.lift(
            (scannable, coreSubscriber) ->
                AeronPublicationSubscriber.create(name + "AeronPublicationSubscriber", workQueueProcessor, coreSubscriber, publication));
    this.receiveFlux = AeronSubscriptionFlux.create(name + "AeronSubscriptionFlux", workQueueProcessor, subscription, allocator);
    this.onClose = MonoProcessor.create();

    onClose
        .doFinally(
            signalType -> {
              publication.close();
              subscription.close();
            })
        .subscribe();
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    return Flux.from(frames)
        .map(Frame::content)
        .transform(lift)
        .then();
  }

  @Override
  public Flux<Frame> receive() {
    return receiveFlux.map(Frame::from).doOnNext(frame -> System.out.println(name + " -^^- received frame "
                                                                                 + frame.toString()));
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  @Override
  public void dispose() {
    onClose.onComplete();
  }
}
