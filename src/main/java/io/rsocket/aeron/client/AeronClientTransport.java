package io.rsocket.aeron.client;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.Subscription;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.aeron.AeronDuplexConnection;
import io.rsocket.aeron.frames.AeronPayloadFlyweight;
import io.rsocket.aeron.frames.FrameType;
import io.rsocket.aeron.frames.SetupCompleteFlyweight;
import io.rsocket.aeron.frames.SetupFlyweight;
import io.rsocket.aeron.reactor.AeronPublicationSubscriber;
import io.rsocket.aeron.reactor.AeronSubscriptionFlux;
import io.rsocket.aeron.util.Constants;
import io.rsocket.transport.ClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.core.publisher.WorkQueueProcessor;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class AeronClientTransport implements ClientTransport {
  private static final Logger logger = LoggerFactory.getLogger(AeronClientTransport.class);
  private static final AtomicInteger STREAM = new AtomicInteger(2);
  private final WorkQueueProcessor<Runnable> workQueueProcessor;
  private final Aeron aeron;
  private final String aeronURL;
  private final ByteBufAllocator allocator;

  public AeronClientTransport(
      WorkQueueProcessor<Runnable> workQueueProcessor,
      Aeron aeron,
      String aeronURL,
      ByteBufAllocator allocator) {
    this.workQueueProcessor = workQueueProcessor;
    this.allocator = allocator;
    this.aeron = aeron;
    this.aeronURL = aeronURL;
  }

  @Override
  public Mono<DuplexConnection> connect() {
    long connectionId = ThreadLocalRandom.current().nextLong();
    int streamId = STREAM.getAndAdd(2);

    MonoProcessor<Void> onImageAvailable = MonoProcessor.create();
    Subscription clientSubscription =
        aeron.addSubscription(
            aeronURL, streamId, image -> onImageAvailable.onComplete(), image -> {});
    return Mono.defer(
        () -> {
          logger.info(
              "creating new connection with connection id {} and aeron stream id {} to url {}",
              connectionId,
              streamId,
              aeronURL);

          MonoProcessor<DuplexConnection> notify = MonoProcessor.create();

          ConcurrentPublication serverManagementPublication =
              aeron.addPublication(aeronURL, Constants.SERVER_MANAGEMENT_STREAM_ID);

          Subscription clientManagementSubscription =
              aeron.addSubscription(
                  aeronURL, Constants.CLIENT_MANAGEMENT_STREAM_ID);

          AeronSubscriptionFlux.create("clientManagementStream", workQueueProcessor, clientManagementSubscription, allocator)
              .filter(
                  byteBuf ->
                      AeronPayloadFlyweight.frameType(byteBuf) == FrameType.SETUP_COMPLETE
                          && SetupCompleteFlyweight.connectionId(byteBuf) == connectionId)
              .flatMap(
                  byteBuf -> {
                    logger.info(
                        "received setup complete for connection id {} and aeron stream id {} to url {}",
                        connectionId,
                        streamId,
                        aeronURL);

                    return Mono.<DuplexConnection>create(
                        sink -> {
                          String serverAeronUrl = SetupCompleteFlyweight.aeronUrl(byteBuf);
                          int serverStreamId = SetupCompleteFlyweight.streamId(byteBuf);

                          logger.info(
                              "connection with id {} creating publication to server with aeron stream id {} to url {}",
                              connectionId,
                              serverStreamId,
                              serverAeronUrl);

                          AeronDuplexConnection connection =
                              new AeronDuplexConnection(
                                  "client",
                                  workQueueProcessor,
                                  aeron.addPublication(serverAeronUrl, serverStreamId),
                                  clientSubscription,
                                  allocator);

                          sink.success(connection);
                        });
                  })
              .doFinally(signalType -> clientManagementSubscription.close())
              .subscribe(notify);

          ByteBuf encode = SetupFlyweight.encode(allocator, connectionId, streamId, aeronURL);

          return Mono.just(encode)
              .transform(
                  Operators.lift(
                      (s, c) ->
                          AeronPublicationSubscriber.create("serverManagementPublication", workQueueProcessor, c, serverManagementPublication)))
              .doFinally(signalType -> serverManagementPublication.close())
              .then(onImageAvailable)
              .then(notify)
              .doOnSuccess(
                  duplexConnection -> {
                    logger.info(
                        "successfully created new connection with connection id {} and aeron stream id {} to url {}",
                        connectionId,
                        streamId,
                        aeronURL);
                  })
              .timeout(Duration.ofSeconds(1000))
              .onErrorMap(
                  throwable -> {
                    clientManagementSubscription.close();
                    logger.error("error trying to make a connection to " + aeronURL, throwable);
                    if (throwable instanceof TimeoutException) {
                      return new TimeoutException(
                          "timed out waiting for connection to server " + aeronURL);
                    } else {
                      return throwable;
                    }
                  });
        });
  }
}
