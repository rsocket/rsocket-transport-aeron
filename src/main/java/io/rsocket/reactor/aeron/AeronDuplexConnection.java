package io.rsocket.reactor.aeron;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOutbound;

public class AeronDuplexConnection implements DuplexConnection {

  private final MonoProcessor<Void> onClose;
  private final AeronInbound inbound;
  private final AeronOutbound outbound;

  public AeronDuplexConnection(AeronInbound inbound, AeronOutbound outbound) {
    this.inbound = inbound;
    this.outbound = outbound;
    this.onClose = MonoProcessor.create();
    // todo: onClose.doFinally(signalType -> { doSomething() }).subscribe();
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    return outbound.send(Flux.from(frames).map(Frame::content).map(this::toByteBuffer)).then();
  }

  @Override
  public Flux<Frame> receive() {
    return inbound.receive().map(this::toByteBuf).map(Frame::from);
  }

  private ByteBuf toByteBuf(ByteBuffer byteBuffer) {
    return Unpooled.wrappedBuffer(byteBuffer);
  }

  private ByteBuffer toByteBuffer(ByteBuf byteBuf) {
    return byteBuf.nioBuffer();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  @Override
  public void dispose() {
    if (!onClose.isDisposed()) {
      onClose.onComplete();
    }
  }
}
