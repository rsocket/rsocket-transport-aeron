package io.rsocket.reactor.aeron;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import java.util.concurrent.atomic.AtomicReference;
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

              AtomicReference reference = new AtomicReference();


              AeronClient client = AeronClient.create("client", options);
              client
                  .newHandler(
                      (inbound, outbound) -> {
                        AeronDuplexConnection duplexConnection =
                            new AeronDuplexConnection(inbound, outbound);
                        duplexConnection.setName("client");

                        reference.set(duplexConnection);
//                        sink.success(duplexConnection);
                        return duplexConnection.onClose();
//                        return Mono.never();
                      })
//                  .then()
                  .subscribe(clientHandler -> {
                    Object o = reference.get();
                    System.err.println(o.getClass());
                    sink.success((DuplexConnection) o);
                  });
            })
        .log("AeronClientTransport connect ");
  }
}
