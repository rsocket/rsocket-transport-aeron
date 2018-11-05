package io.rsocket.reactor.aeron;

import io.rsocket.transport.ServerTransport;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronOptions;

public class AeronServerTransport implements ServerTransport<AeronServerWrapper> {

  private final Consumer<AeronOptions> aeronOptions;

  public AeronServerTransport(Consumer<AeronOptions> aeronOptions) {
    this.aeronOptions = aeronOptions;
  }

  @Override
  public Mono<AeronServerWrapper> start(ConnectionAcceptor acceptor) {
    return Mono.fromSupplier(() -> new AeronServerWrapper(acceptor, aeronOptions));
  }
}
