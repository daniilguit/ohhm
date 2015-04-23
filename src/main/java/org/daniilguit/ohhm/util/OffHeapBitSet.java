package org.daniilguit.ohhm.util;

import java.nio.ByteBuffer;

/**
 * Created by Daniil Gitelson on 23.04.15.
 */
public class OffHeapBitSet {
    private final ByteBuffer buffer;

    public OffHeapBitSet(long maxValue) {
        assert maxValue / 8 + 1 < Integer.MAX_VALUE;

        this.buffer = ByteBuffer.allocateDirect((int) (maxValue / 8) + 1);
    }

    public void set(long at) {
        checkIndex(at);
        this.buffer.put(index(at), (byte) (this.buffer.get(index(at)) | bit(at)));
    }

    public boolean test(long at) {
        checkIndex(at);
        return (buffer.get(index(at)) & bit(at)) != 0;
    }

    private void checkIndex(long at) {
        if (at < 0 || at / 8 > buffer.remaining()) {
            throw new IndexOutOfBoundsException();
        }
    }

    private static byte bit(long at) {
        return (byte) (1 << (at % 8));
    }

    private static int index(long at) {
        return (int) (at / 8);
    }
}
