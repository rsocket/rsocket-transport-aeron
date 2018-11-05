package io.rsocket.reactor.aeron;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.transport.ServerTransport.ConnectionAcceptor;
import reactor.core.publisher.Mono;

public class ServerRunner {

  public static void main(String[] args) {
//    ConnectionAcceptor acceptor = new ConnectionAcceptor() {
//      @Override
//      public Mono<Void> apply(DuplexConnection duplexConnection) {
//        return null;
//      }
//    };

    RSocketFactory
        .receive()
        .acceptor((setup, sendingSocket) -> {
          System.exit(42);  // todo help me!
          return null;
        })
        .transport(() -> new AeronServerTransport(options -> {}))
    ;





  }
}
