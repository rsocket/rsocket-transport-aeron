package io.rsocket.reactor.aeron;

import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.client.AeronClient;

public class RawAeronClientRunner {

  public static void main(String[] args) throws Exception {

    AeronClient client =
        AeronClient.create(
            "client",
            options -> {
              options.serverChannel(Channels.serverChannel);
              options.clientChannel(Channels.clientChannel);
            });
    client
        .newHandler(
            (inbound, outbound) -> {
              inbound
                  .receive()
                  .asString()
                  .log("client receive -> ")
                  .doOnError(Throwable::printStackTrace)
                  .subscribe();

              outbound
                  .send(
                      Flux.range(1, 10000)
                          .delayElements(Duration.ofSeconds(3))
                          .map(i -> AeronUtils.stringToByteBuffer("" + i))
                          .log("client send -> "))
                  .then()
                  .subscribe();

              return Mono.never();
            })
        .block();

    System.out.println("main completed");
  }
}
