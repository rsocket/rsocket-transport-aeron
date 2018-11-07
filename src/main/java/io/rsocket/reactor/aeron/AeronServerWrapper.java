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

    System.err.println("AeronServerWrapper create server");
    server = AeronServer.create("server", aeronOptions);
    server
        .newHandler(
            (inbound, outbound) -> {
              System.err.println(
                  "AeronServerWrapper handler triggers only when a client connects to server");
              AeronDuplexConnection duplexConnection = new AeronDuplexConnection(inbound, outbound);
              return acceptor
                  .apply(duplexConnection)
                  .log("AeronServerWrapper Acceptor apply duplex ")
                  .doOnSuccess(s -> System.err.println("AeronServerWrapper is started"))
                  .then(
                      onClose.doFinally(
                          s -> {
                            System.err.println("AeronServerWrapper duplexConnection.dispose()");
                            duplexConnection.dispose();
                          }));
            })
        .log("AeronServerWrapper newHandler ")
        .block(); // todo fix it

    System.err.println("AeronServerWrapper finishes its setup");
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
