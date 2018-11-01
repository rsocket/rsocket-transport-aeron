package io.rsocket.aeron.server;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.Subscription;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.aeron.AeronDuplexConnection;
import io.rsocket.aeron.frames.AeronPayloadFlyweight;
import io.rsocket.aeron.frames.FrameType;
import io.rsocket.aeron.frames.SetupCompleteFlyweight;
import io.rsocket.aeron.frames.SetupFlyweight;
import io.rsocket.aeron.reactor.AeronPublicationSubscriber;
import io.rsocket.aeron.reactor.AeronSubscriptionFlux;
import io.rsocket.aeron.util.Constants;
import io.rsocket.transport.ServerTransport;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.core.publisher.WorkQueueProcessor;

public class AeronServerTransport implements ServerTransport<Closeable> {
  private static final Logger logger = LoggerFactory.getLogger(AeronServerTransport.class);
  private final WorkQueueProcessor<Runnable> workQueueProcessor;
  private final Aeron aeron;
  private final String aeronURL;
  private final ByteBufAllocator allocator;

  public AeronServerTransport(WorkQueueProcessor<Runnable> workQueueProcessor, Aeron aeron,
      String aeronURL, ByteBufAllocator allocator) {
    this.workQueueProcessor = workQueueProcessor;
    this.aeron = aeron;
    this.aeronURL = aeronURL;
    this.allocator = allocator;
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    return Mono.fromSupplier(() -> {
      return new AeronServer(workQueueProcessor, aeron, aeronURL, allocator, acceptor);
    }).cast(Closeable.class);
  }

  public static class AeronServer implements Closeable {
    private static final AtomicInteger STREAM = new AtomicInteger(1);
    private final WorkQueueProcessor<Runnable> workQueueProcessor;
    private final Aeron aeron;
    private final String aeronURL;
    private final ByteBufAllocator allocator;
    private final ConnectionAcceptor acceptor;

    private final MonoProcessor<Void> onClose;

    public AeronServer(WorkQueueProcessor<Runnable> workQueueProcessor, Aeron aeron,
        String aeronURL, ByteBufAllocator allocator, ConnectionAcceptor acceptor) {
      this.workQueueProcessor = workQueueProcessor;
      this.aeron = aeron;
      this.aeronURL = aeronURL;
      this.allocator = allocator;
      this.onClose = MonoProcessor.create();
      this.acceptor = acceptor;

      logger.info("starting aeron server for url {}", aeronURL);

      Disposable disposable = Flux.defer(() -> {
        Subscription serverManagementSubscription = aeron.addSubscription(aeronURL,
            Constants.SERVER_MANAGEMENT_STREAM_ID);

        return AeronSubscriptionFlux.create("serverManagementStream", workQueueProcessor,
            serverManagementSubscription, allocator).filter(byteBuf -> {
              return AeronPayloadFlyweight.frameType(byteBuf) == FrameType.SETUP;
            }).flatMap(this::handleNextConnection).doFinally(s -> {
              serverManagementSubscription.close();
            });
      }).doOnError(throwable -> logger.error("error receiving connections {}", throwable)).retry()
          .subscribe();

      onClose.doFinally(s -> disposable.dispose()).subscribe();
    }

    private Mono<Void> handleNextConnection(ByteBuf byteBuf) {
      return Mono.defer(() -> {
        String aeronClientUrl = SetupFlyweight.aeronUrl(byteBuf);
        long connectionId = SetupFlyweight.connectionId(byteBuf);
        int clientStreamId = SetupFlyweight.streamId(byteBuf);
        int serverStreamId = STREAM.getAndAdd(2);

        logger.info("receiving connection with id {} from aeron stream id {} and url {}",
            connectionId, clientStreamId, aeronClientUrl);

        ConcurrentPublication clientManagementPublication = aeron.addPublication(aeronClientUrl,
            Constants.CLIENT_MANAGEMENT_STREAM_ID);

        ConcurrentPublication clientPublication = aeron.addPublication(aeronClientUrl,
            clientStreamId);

        aeron.addSubscription(aeronURL, serverStreamId, i -> {
          AeronDuplexConnection connection = new AeronDuplexConnection("server", workQueueProcessor,
              clientPublication, i.subscription(), allocator);

          acceptor.apply(connection);

          logger.info("successfully received connection with id {} with aeron url {}", connectionId,
              aeronClientUrl);
        }, i -> {
        });
        logger.info("received connection with id {} from aeron url {}", connectionId,
            aeronClientUrl);

        ByteBuf encode = SetupCompleteFlyweight.encode(allocator, connectionId, serverStreamId,
            aeronURL);

        return Mono.just(encode)
            .doOnNext(s -> logger.info(
                "sending setup complete with connection id {} with aeron stream id {} and url {}",
                connectionId, serverStreamId, aeronClientUrl))
            .transform(Operators
                .lift((s, c) -> AeronPublicationSubscriber.create("serverSetupSubscription",
                    workQueueProcessor, c, clientManagementPublication)))
            .doFinally(s -> clientManagementPublication.close()).then();
      });
    }

    @Override
    public Mono<Void> onClose() {
      return onClose;
    }

    @Override
    public void dispose() {
      if (!onClose.isDisposed()) {
        onClose.onComplete();
      }
    }

    public String getAeronURL() {
      return aeronURL;
    }
  }
}
