package io.rsocket.aeron.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.rsocket.util.NumberUtils;
import java.nio.charset.StandardCharsets;

public final class SetupCompleteFlyweight {
  private SetupCompleteFlyweight() {
  }

  public static ByteBuf encode(ByteBufAllocator allocator, long connectionId, int streamId,
      String aeronUrl) {

    ByteBuf byteBuf = AeronPayloadFlyweight.encode(allocator, FrameType.SETUP_COMPLETE);

    byteBuf.writeLong(connectionId).writeInt(streamId);

    int serviceLength = NumberUtils.requireUnsignedShort(ByteBufUtil.utf8Bytes(aeronUrl));
    byteBuf.writeShort(serviceLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, aeronUrl, serviceLength);

    return byteBuf;
  }

  public static long connectionId(ByteBuf byteBuf) {
    int offset = AeronPayloadFlyweight.headerOffset();
    return byteBuf.getLong(offset);
  }

  public static int streamId(ByteBuf byteBuf) {
    int offset = AeronPayloadFlyweight.headerOffset() + Long.BYTES;
    return byteBuf.getInt(offset);
  }

  public static String aeronUrl(ByteBuf byteBuf) {
    int offset = AeronPayloadFlyweight.headerOffset() + Long.BYTES + Integer.BYTES;

    short length = byteBuf.getShort(offset);
    offset += Short.BYTES;

    return byteBuf.toString(offset, length, StandardCharsets.UTF_8);
  }
}
