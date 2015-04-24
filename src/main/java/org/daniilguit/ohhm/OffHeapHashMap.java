package org.daniilguit.ohhm;

import org.daniilguit.ohhm.util.BufferUtils;

import java.nio.ByteBuffer;

/**
 * Created by Daniil Gitelson on 23.04.15.
 */
public class OffHeapHashMap {
    private final OffHeapHashMapPartition[] partitions;

    public OffHeapHashMap(OffHeapHashMapConfig config) {
        partitions = new OffHeapHashMapPartition[config.partitions];

        for (int i = 0; i < partitions.length; i++) {
            partitions[i] = new OffHeapHashMapPartition(config);
        }
    }

    public OffHeapHashMapPartition.Entry entry(ByteBuffer key) {
        int hash = key.hashCode();
        return partitions[partitions.length].entry(key, hash);
    }

    private class OffHeapHashMapPartition {
        private final HashTable hashTable;
        private final CompactingBuffer compactingBuffer;

        public OffHeapHashMapPartition(OffHeapHashMapConfig config) {
            hashTable = new HashTable(config.initialSize / config.partitions);
            compactingBuffer = new CompactingBuffer(config.bucketSize);
        }

        public class Entry implements HashTable.LookupPredicate {
            final int hash;
            final ByteBuffer key;
            long location;
            boolean located;

            public Entry(int hash, ByteBuffer key) {
                this.hash = hash;
                this.key = key;
            }

            public ByteBuffer get() {
                ByteBuffer entry = compactingBuffer.compute(
                        () -> location = hashTable.get(hash, this),
                        buffer -> ByteBuffer.allocate(buffer.remaining()).put(buffer)
                );
                entry.position(0);
                int keySize = BufferUtils.readPackedInt(entry);
                BufferUtils.skip(entry, keySize);
                return entry;
            }

            public void put(ByteBuffer value) {
                if (located) {
                    location = compactingBuffer.append(value, location);
                } else {
                    location = hashTable.get(hash, this);
                }
            }

            @Override
            public boolean check(long location) {
                return compactingBuffer.<Boolean>compute(() -> location, buffer -> {
                    int size = BufferUtils.readPackedInt(buffer);
                    if (size != key.remaining()) {
                        return false;
                    }
                    buffer.limit(buffer.position() + size);
                    return buffer.equals(key);
                });
            }
        }

        public Entry entry(ByteBuffer key, int hash) {
            return new Entry(hash, key);
        }
    }
}
