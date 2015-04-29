package org.daniilguit.ohhm;

import org.daniilguit.ohhm.util.DirectBuffer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Daniil Gitelson on 23.04.15.
 */
public class HashTable {
    private static final int TABLE_ENTRY_SIZE = 4;
    private static final int VALUE_ENTRY_SIZE = 16;

    public static final int MIN_TABLE_SIZE = 17;

    private final AtomicInteger count = new AtomicInteger();
    private final AtomicInteger chainsCount = new AtomicInteger();

    private final DirectBuffer table;
    private final DirectBuffer chains;
    private final int entries;

    public interface LookupPredicate {

        boolean check(long location);
    }
    public HashTable(int size) {
        entries = Math.max(size, MIN_TABLE_SIZE);
        table = new DirectBuffer(entries * TABLE_ENTRY_SIZE);
        chains = new DirectBuffer(entries * VALUE_ENTRY_SIZE);
    }

    public int size() {
        return entries;
    }

    public double density() {
        return 1.0 * chainsCount.get() / entries;
    }

    public boolean insert(HashTable table) {
        int limit = table.chainsCount.get() * VALUE_ENTRY_SIZE;
        for (int offset = 0; offset < limit; offset += VALUE_ENTRY_SIZE) {
            int hash = table.getHashAtOffset(offset);
            long location = table.getLocationAtOffset(offset);
            if (location >= 0) {
                if (!insert(hash, location)) {
                    return false;
                }
            }
        }
        return true;
    }

    public long get(int hash, LookupPredicate predicate) {
        int offset = find(hash, predicate);
        if (offset >= 0) {
            return getLocationAtOffset(offset);
        }
        return -1;
    }

    public boolean contains(int hash, long location) {
        for (int offset = getStartOffsetInChain(hash); offset >= 0; offset = getNextInChain(offset)) {
            int entryHash = getHashAtOffset(offset);
            if (hash != entryHash) {
                continue;
            }
            long locationAtOffset = getLocationAtOffset(offset);
            if (locationAtOffset == location) {
                return true;
            }
        }

        return false;

    }

    public int find(int hash, LookupPredicate predicate) {
        for (int offset = getStartOffsetInChain(hash); offset >= 0; offset = getNextInChain(offset)) {
            int entryHash = getHashAtOffset(offset);
            if (hash != entryHash) {
                continue;
            }
            long locationAtOffset = getLocationAtOffset(offset);
            if (locationAtOffset >= 0) {
                if (predicate.check(locationAtOffset)) {
                    return offset;
                }
            }
        }

        return -1;
    }

    public boolean insert(int hash, long location) {
        int newEntryOffset = allocateEntry(hash);
        if (newEntryOffset < 0) {
            return false;
        }
        setEntryAtOffset(newEntryOffset, hash, location);

        int offset = getStartOffsetInChain(hash);
        if (offset < 0) {
            if (setStartOffsetInChain(newEntryOffset, hash)) {
                return true;
            }
        }
        while (offset < chains.size()){
            int nextOffset = getNextInChain(offset);
            if (nextOffset < 0) {
                setNextInChain(offset, newEntryOffset);
                return true;
            }
            offset = nextOffset;
        }
        return false;
    }

    public int update(int hash, long oldLocation, long location) {
        for (int offset = getStartOffsetInChain(hash); offset >= 0; offset = getNextInChain(offset)) {
            if (hash == getHashAtOffset(offset) && setLocationAtOffset(offset, oldLocation, location)) {
                return offset;
            }
        }
        return -1;
    }

    public int upsert(int hash, LookupPredicate predicate, long location) {
        int offset = find(hash, predicate);
        if (offset >= 0) {
            setLocationAtOffset(offset, location);
            return offset;
        }
        offset = getStartOffsetInChain(hash);
        int newEntryOffset = allocateEntry(hash);
        if (newEntryOffset < 0) {
            return  -1;
        }
        setEntryAtOffset(newEntryOffset, hash, location);

        if (offset < 0) {
            if (setStartOffsetInChain(newEntryOffset, hash)) {
                return newEntryOffset;
            }
        }
        for (offset = getStartOffsetInChain(hash); offset >= 0; offset = getNextInChain(offset)) {
            int entryHash = getHashAtOffset(offset);
            if (hash == entryHash) {
                long locationAtOffset = getLocationAtOffset(offset);
                if (locationAtOffset >= 0) {
                    if (predicate.check(locationAtOffset)) {
                        setLocationAtOffset(offset, locationAtOffset, location);
                        return offset;
                    }
                }
            }
            if (setNextInChain(offset, newEntryOffset)) {
                return newEntryOffset;
            }
        }
        return -1;
    }

    private int allocateEntry(int hash) {
        int index = chainsCount.getAndIncrement();
        if (index >= entries) {
            return -1;
        }
        return index * VALUE_ENTRY_SIZE;
    }

    private int getStartOffsetInChain(int hash) {
        return table.getIntVolatile(bucket(hash) * TABLE_ENTRY_SIZE) - 1;
    }

    private boolean setStartOffsetInChain(int offset, int hash) {
        return table.compareAndSwapInt(bucket(hash) * TABLE_ENTRY_SIZE, 0, offset + 1);
    }

    private boolean setNextInChain(int offset, int newEntryOffset) {
        return chains.compareAndSwapInt(checkOffset(offset) + 12, 0, newEntryOffset + 1);
    }

    private int getNextInChain(int offset) {
        return checkOffset(chains.getIntVolatile(checkOffset(offset) + 12) - 1);
    }

    public long getLocationAtOffset(int offset) {
        return chains.getLongVolatile(checkOffset(offset) + 4) - 1;
    }

    public int getHashAtOffset(int offset) {
        return chains.getIntVolatile(checkOffset(offset));
    }

    private void setEntryAtOffset(int offset, int hash, long location) {
        chains.putIntVolatile(checkOffset(offset), hash);
        chains.putLongVolatile(offset + Integer.BYTES, location + 1);
    }

    public void setLocationAtOffset(int offset, long location) {
        chains.putLongVolatile(checkOffset(offset) + Integer.BYTES, location + 1);
    }

    public boolean setLocationAtOffset(int offset, long previous, long location) {
        return chains.compareAndSwapLong(checkOffset(offset) + Integer.BYTES, previous + 1, location + 1);
    }

    private int checkOffset(int offset) {
        assert offset == -1 || offset % VALUE_ENTRY_SIZE == 0;
        return offset;
    }

    private int bucket(int hash) {
        return (Math.abs(hash) % entries);
    }

}
