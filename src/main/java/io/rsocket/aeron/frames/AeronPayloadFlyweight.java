package io.rsocket.aeron.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public final class AeronPayloadFlyweight {
  private static final short VERSION = 0;

  private AeronPayloadFlyweight() {}

  public static FrameType frameType(ByteBuf byteBuf) {
    int anInt = byteBuf.getInt(Short.BYTES);

    return FrameType.fromEncodedType(anInt);
  }

  public static short version(ByteBuf byteBuf) {
    return byteBuf.getShort(0);
  }

  public static ByteBuf encode(ByteBufAllocator allocator, FrameType type) {
    return allocator.buffer().writeShort(VERSION).writeInt(type.getEncodedType());
  }

  public static int headerOffset() {
    return Short.BYTES + Integer.BYTES;
  }
}
