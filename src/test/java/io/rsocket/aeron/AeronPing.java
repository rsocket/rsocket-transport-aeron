/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.aeron;

import io.aeron.Aeron;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.aeron.client.AeronClientTransport;
import io.rsocket.test.PingClient;
import org.HdrHistogram.Recorder;
import reactor.core.publisher.Mono;
import reactor.core.publisher.WorkQueueProcessor;

import java.time.Duration;

public final class AeronPing {

  public static void main(String... args) {
    Aeron aeron = Aeron.connect();
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    WorkQueueProcessor<Runnable> workQueueProcessor = WorkQueueProcessor.create();
    workQueueProcessor
        .doOnNext(Runnable::run)
        .doOnError(Throwable::printStackTrace)
        .onErrorResume(t -> Mono.empty())
        .subscribe();

    String aeronUrl = "aeron:udp?endpoint=127.0.0.1:39790";

    AeronClientTransport aeronTransportClient =
        new AeronClientTransport(workQueueProcessor, aeron, aeronUrl, allocator);

    Mono<RSocket> client = RSocketFactory.connect().transport(aeronTransportClient).start();
    PingClient pingClient = new PingClient(client);
    Recorder recorder = pingClient.startTracker(Duration.ofSeconds(1));
    final int count = 1_000_000_000;
    pingClient
        .startPingPong(count, recorder)
        .doOnTerminate(() -> System.out.println("Sent " + count + " messages."))
        .blockLast();

    System.exit(0);
  }
}
