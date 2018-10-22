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
import io.rsocket.Closeable;
import io.rsocket.aeron.client.AeronClientTransport;
import io.rsocket.aeron.server.AeronServerTransport;
import io.rsocket.test.ClientSetupRule;
import reactor.core.publisher.Mono;
import reactor.core.publisher.WorkQueueProcessor;

class AeronClientSetupRule extends ClientSetupRule<String, Closeable> {

  public static final String aeronUrl = "aeron:udp?endpoint=127.0.0.1:39790";
  private static final AeronServerTransport server;
  private static final AeronClientTransport client;

  static {
    MediaDriverHolder.getInstance();
    Aeron aeron = Aeron.connect();

    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    WorkQueueProcessor<Runnable> workQueueProcessor = WorkQueueProcessor.create();
    workQueueProcessor
        .doOnNext(Runnable::run)
        .doOnError(Throwable::printStackTrace)
        .onErrorResume(t -> Mono.empty())
        .subscribe();

    server = new AeronServerTransport(workQueueProcessor, aeron, aeronUrl, allocator);
    client = new AeronClientTransport(workQueueProcessor, aeron, aeronUrl, allocator);
  }

  AeronClientSetupRule() {
    super(() -> aeronUrl, (address, server) -> client, address -> server);
  }
}
