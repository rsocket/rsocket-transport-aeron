package io.rsocket.reactor.aeron;

import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.server.AeronServer;

public class RawAeronServerRunner {

  public static void main(String[] args) throws Exception {

    AeronServer server =
        AeronServer.create("server", options -> options.serverChannel(Channels.serverChannel));
    server
        .newHandler(
            (inbound, outbound) -> {
              System.err.println("RawAeronServerRunner add duplex conn to handler");
              inbound.receive().asString().log("server receive -> ").subscribe();

              outbound
                  .send(
                      Flux.range(1, 10000)
                          .delayElements(Duration.ofMillis(250))
                          .map(i -> AeronUtils.stringToByteBuffer("" + i))
                          .log("server send -> "))
                  .then()
                  .subscribe();
              return Mono.never();
            })
        .block();

    System.out.println("main finished");
  }
}
