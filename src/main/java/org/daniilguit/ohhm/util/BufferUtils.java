package org.daniilguit.ohhm.util;

import java.nio.ByteBuffer;

/**
 * Created by Daniil Gitelson on 17.04.15.
 */
public class BufferUtils {
    public static ByteBuffer locate(ByteBuffer buffer, int position, int length) {
        buffer.limit(position + length);
        buffer.position(position);
        return buffer;
    }

    public static void skip(ByteBuffer dest, int length) {
        dest.position(dest.position() + length);
    }

    public static void writeBuffer(ByteBuffer dest, ByteBuffer src) {
        int position = src.position();
        dest.put(src);
        src.position(position);
    }


    public static int packedLongSize(long value) {
        int bits = Long.SIZE - Long.numberOfLeadingZeros(value);
        return Math.max(bits / 7 + (bits % 7 != 0 ? 1 : 0), 1);
    }

    public static int writePackedLong(ByteBuffer output, long value) {
        assert value >= 0; //
        int position = output.position();
        while (value > 127) {
            output.put((byte) ((value & 127) + ((value > 127 ? 1 : 0) << 7)));
            value >>= 7;
        }
        output.put((byte) (value & 127));
        return position;
    }

    public static int readPackedInt(ByteBuffer input) {
        return (int) readPackedLong(input);
    }

    public static long readPackedLong(ByteBuffer input) {
        long result = 0;
        long read;
        long index = 0;
        do {
            read = input.get() & 0xFF;
            result += (read & 127) << index;
            index += 7;
        } while (read > 127);
        assert result >= 0;
        return result;
    }
}
