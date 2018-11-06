package io.rsocket.reactor.aeron;

import static java.lang.Boolean.TRUE;

import io.aeron.ChannelUriStringBuilder;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ServerRunner {

  public static void main(String[] args) throws InterruptedException {

    String serverChannel =
        new ChannelUriStringBuilder()
            .reliable(TRUE)
            .media("udp")
            .endpoint("localhost:13000")
            .build();

    String clientChannel =
        new ChannelUriStringBuilder()
            .reliable(TRUE)
            .media("udp")
            .endpoint("localhost:12001")
            .build();

    RSocketFactory.receive()
        .acceptor(
            (setup, reactiveSocket) ->
                Mono.just(
                    new AbstractRSocket() {
                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        System.err.println("requestStream: " + payload.getDataUtf8());
                        return Flux.interval(Duration.ofMillis(100))
                            .log("send back ")
                            .map(aLong -> DefaultPayload.create("Interval: " + aLong));
                      }
                    }))
        //        .transport(TcpServerTransport.create("localhost", 7000))
        .transport(() -> new AeronServerTransport(options -> options.serverChannel(serverChannel)))
        .start()
        .block();

    System.err.println("start 2");

    RSocket socket =
        RSocketFactory.connect()
//                        .transport(() -> TcpClientTransport.create("localhost", 7000))
            .transport(
                () ->
                    new AeronClientTransport(
                        options -> {
                          options.serverChannel(serverChannel);
                          options.clientChannel(clientChannel);
                        }))
            .start().log("RSocketFactory ### ")
            .block();

    System.err.println("start " + socket);

    socket
        .requestStream(DefaultPayload.create("Hello"))
        .log("receive ")
        .map(Payload::getDataUtf8)
        .doOnNext(System.out::println)
        .take(10)
        .then()
        .doFinally(signalType -> socket.dispose())
        .then()
        .block();

    System.err.println("join");
    Thread.currentThread().join();
  }
}
