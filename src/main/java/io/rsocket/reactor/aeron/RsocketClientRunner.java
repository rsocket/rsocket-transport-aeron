package io.rsocket.reactor.aeron;

import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.util.DefaultPayload;

public class RsocketClientRunner {

  public static void main(String[] args) throws Exception {

    // start client
    RSocketFactory.connect()
        .transport(
            () ->
                new AeronClientTransport(
                    options -> {
                      options.serverChannel(Channels.serverChannel);
                      options.clientChannel(Channels.clientChannel);
                    }))
        .start()
        .subscribe(
            rSocket -> {
              System.err.println("start " + rSocket);

              rSocket
                  .requestStream(DefaultPayload.create("Hello"))
                  .log("receive ")
                  .map(Payload::getDataUtf8)
                  .doOnNext(System.out::println)
                  .take(10)
                  .then()
                  .doFinally(signalType -> rSocket.dispose())
                  .then()
                  .subscribe();
            });

    System.err.println("wait for the end");
    Thread.currentThread().join();
  }
}
