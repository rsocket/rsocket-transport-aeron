package io.rsocket.aeron;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.BufferClaim;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.aeron.reactor.AeronPublicationSubscriber;
import io.rsocket.aeron.reactor.AeronSubscriptionFlux;
import java.nio.ByteBuffer;
import java.util.function.Function;
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOutbound;

public class AeronDuplexConnection2 implements DuplexConnection {

  private static final ThreadLocal<BufferClaim> BUFFER_CLAIMS = ThreadLocal
      .withInitial(BufferClaim::new);
  private static final ThreadLocal<UnsafeBuffer> UNSAFE_BUFFER = ThreadLocal
      .withInitial(UnsafeBuffer::new);

  private final String name;
//  private final Function<? super Publisher<ByteBuf>, ? extends Publisher<ByteBuf>> lift;
//  private final AeronSubscriptionFlux receiveFlux;
  private final MonoProcessor<Void> onClose;
  private final AeronInbound inbound;
  private final AeronOutbound outbound;

  public AeronDuplexConnection2(String name, AeronInbound inbound, AeronOutbound outbound) {
    this.name = name;
    this.inbound = inbound;
    this.outbound = outbound;

    this.onClose = MonoProcessor.create();

    // todo
//    onClose.doFinally(signalType -> {
//      inbound.close();
//      outbound.close();
//    }).subscribe();
  }


//  public AeronDuplexConnection2(String name, WorkQueueProcessor<Runnable> workQueueProcessor,
//      Publication publication, Subscription subscription, ByteBufAllocator allocator) {
//    this.name = name;
//    this.lift = Operators.lift((scannable, coreSubscriber) -> AeronPublicationSubscriber.create(
//        name + "AeronPublicationSubscriber", workQueueProcessor, coreSubscriber, publication));
//    this.receiveFlux = AeronSubscriptionFlux.create(name + "AeronSubscriptionFlux",
//        workQueueProcessor, subscription, allocator);
//    this.onClose = MonoProcessor.create();
//
//    onClose.doFinally(signalType -> {
//      publication.close();
//      subscription.close();
//    }).subscribe();
//  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
//    return Flux.from(frames).map(Frame::content)
//        .map(byteBuf -> {
//
//        }).transform(lift).then();
    return null;
  }

  @Override
  public Flux<Frame> receive() {
    return inbound.receive().map(this::toByteBuf).map(Frame::from);
  }

  private ByteBuf toByteBuf(ByteBuffer byteBuffer) {
    // todo it
    return null;
  }

  private ByteBuffer toByteBuffer(ByteBuf byteBuf) {
    // todo it
    return null;
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
