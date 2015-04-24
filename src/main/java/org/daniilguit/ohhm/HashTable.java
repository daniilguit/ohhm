package org.daniilguit.ohhm;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Daniil Gitelson on 23.04.15.
 */
public class HashTable {
    private static final int ENTRY_SIZE = 12;
    private static final int BUCKET_SIZE = 16;

    private final LockManager lockManager = new LockManager();
    private final AtomicInteger count = new AtomicInteger();
    private volatile ByteBuffer table;

    public interface LookupPredicate {
        boolean check(long location);
    }

    public HashTable(int initialSize) {
        table = allocateBuffer(initialSize);
    }

    public void grow() {
        ByteBuffer table = this.table;
        ByteBuffer newTable = allocateBuffer(table.remaining() * 2 - 1);
        for (int offset = 0; offset < table.remaining(); offset += ENTRY_SIZE) {
            int hash = table.get(offset);
            long location = table.getLong(offset + Integer.BYTES);
            if (location >= 0) {

            }
        }
    }

    private ByteBuffer allocateBuffer(int initialSize) {
        return ByteBuffer.allocateDirect(initialSize * ENTRY_SIZE);
    }

    public long get(int hash, LookupPredicate predicate) {
        ByteBuffer table = this.table;
        int index = bucket(hash, table);
        int endOffset = (index + 1) * BUCKET_SIZE * ENTRY_SIZE;
        int offset;
        for (offset = index * ENTRY_SIZE * BUCKET_SIZE; offset < endOffset; offset += ENTRY_SIZE) {
            long location = table.getLong(offset + Integer.BYTES);
            if (table.getInt(offset) == hash && predicate.check(location)) {
                return location;
            }
        }
        return -1;
    }

    public boolean put(int hash, LookupPredicate predicate, long newLocation) {
        ByteBuffer table = this.table;
        int index = bucket(hash, table);
        int endOffset = (index + 1) * BUCKET_SIZE * ENTRY_SIZE;
        int offset;
        for (offset = index * ENTRY_SIZE * BUCKET_SIZE; offset < endOffset; offset += ENTRY_SIZE) {
            long location = table.getLong(offset + Integer.BYTES);
            if (table.getInt(offset) == hash && predicate.check(location)) {
                table.putLong(offset + Integer.BYTES, newLocation);
                return true;
            }
        }
        if (offset < endOffset) {
            table.putInt(offset, hash);
            table.putLong(offset + Integer.BYTES, newLocation);
            return true;
        }
        return false;
    }

    private int bucket(int hash, ByteBuffer table) {
        return (hash % (table.limit() / ENTRY_SIZE)) / BUCKET_SIZE;
    }
}
