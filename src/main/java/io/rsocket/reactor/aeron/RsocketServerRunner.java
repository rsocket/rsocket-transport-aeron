package io.rsocket.reactor.aeron;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RsocketServerRunner {

  public static void main(String[] args) throws Exception {

    // start server
    RSocketFactory.receive()
        .acceptor(
            (setup, reactiveSocket) ->
                Mono.just(
                    new AbstractRSocket() {
                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        System.err.println(
                            "requestStream(), receive request: " + payload.getDataUtf8());
                        return Flux.interval(Duration.ofMillis(100))
                            .log("send back ")
                            .map(aLong -> DefaultPayload.create("Interval: " + aLong));
                      }
                    }))
        .transport(
            () ->
                new AeronServerTransport(options -> options.serverChannel(Channels.serverChannel)))
        .start()
        .subscribe();

    System.err.println("wait for the end");
    Thread.currentThread().join();
  }
}
