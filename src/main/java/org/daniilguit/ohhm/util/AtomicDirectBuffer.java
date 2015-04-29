package org.daniilguit.ohhm.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * Created by Daniil Gitelson on 26.04.15.
 */
public class AtomicDirectBuffer {
    private static final Unsafe unsafe;

    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = (Unsafe) theUnsafe.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private final int size;
    private final ByteBuffer buffer;
    private final long address;

    public AtomicDirectBuffer(int size) {
        this.size = size;
        buffer = ByteBuffer.allocateDirect(size);
        address = ((sun.nio.ch.DirectBuffer) buffer).address();
    }

    public int size() {
        return size;
    }

    private int checkOffsetForInt(int offset) {
        if (offset < 0 || offset + Integer.BYTES > size) {
            throw new IndexOutOfBoundsException(String.format("%s not in [0..%s]", offset + Integer.BYTES, size));
        }
        return offset;
    }

    private int checkOffsetForLong(int offset) {
        if (offset < 0 || offset + Long.BYTES > size) {
            throw new IndexOutOfBoundsException(String.format("%s not in [0..%s]", offset + Long.BYTES, size));
        }
        return offset;
    }

    public boolean compareAndSwapInt(int offset, int old, int value) {
        return unsafe.compareAndSwapInt(null, address + checkOffsetForInt(offset), old, value);
    }

    public boolean compareAndSwapLong(int offset, long old, long value) {
        return unsafe.compareAndSwapLong(null, address + checkOffsetForLong(offset), old, value);
    }

    public int getIntVolatile(int offset) {
        return unsafe.getIntVolatile(null, address + checkOffsetForInt(offset));
    }

    public long getLongVolatile(int offset) {
        return unsafe.getLongVolatile(null, address + checkOffsetForLong(offset));
    }

    public void putIntVolatile(int offset, int value) {
        unsafe.putIntVolatile(null, address + checkOffsetForInt(offset),value);
    }

    public void putLongVolatile(int offset, long value) {
        unsafe.putLongVolatile(null, address + checkOffsetForLong(offset), value);
    }
}
