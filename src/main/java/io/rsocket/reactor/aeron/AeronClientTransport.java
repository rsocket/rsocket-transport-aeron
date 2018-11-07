package io.rsocket.reactor.aeron;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.client.AeronClient;
import reactor.ipc.aeron.client.AeronClientOptions;

public class AeronClientTransport implements ClientTransport {

  private final Consumer<AeronClientOptions> options;

  public AeronClientTransport(Consumer<AeronClientOptions> options) {
    this.options = options;
  }

  @Override
  public Mono<DuplexConnection> connect() {
    return Mono.<DuplexConnection>create(
            sink -> {
              AeronClient client = AeronClient.create("client", options);
              client
                  .newHandler(
                      (inbound, outbound) -> {
                        AeronDuplexConnection duplexConnection =
                            new AeronDuplexConnection(inbound, outbound);
                        sink.success(duplexConnection);
                        return duplexConnection.onClose();
                      })
                  .subscribe(); // todo fix it
            })
        .log("AeronClientTransport connect ");
  }
}
