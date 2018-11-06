package io.rsocket.reactor.aeron;

import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport.ConnectionAcceptor;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.server.AeronServer;

public class AeronServerWrapper implements Closeable {
  private final AeronServer server;
  private final ConnectionAcceptor acceptor;

  private final MonoProcessor<Void> onClose;

  public AeronServerWrapper(ConnectionAcceptor acceptor, Consumer<AeronOptions> aeronOptions) {
    this.acceptor = acceptor;
    this.onClose = MonoProcessor.create();
    // todo: onClose.doFinally(signalType -> { doSomething() }).subscribe();

    server = AeronServer.create("server", aeronOptions);
    server.newHandler(
        (inbound, outbound) -> {
          AeronDuplexConnection duplexConnection = new AeronDuplexConnection(inbound, outbound);
          duplexConnection.setName("server");
          acceptor.apply(duplexConnection)
              .subscribe(); // todo fix it
          return Mono.never();

//          return onClose.doFinally(s -> {
//            System.err.println("duplexConnection.dispose()");
//            duplexConnection.dispose();
//          });
        })
        .block(); // todo fix it

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
