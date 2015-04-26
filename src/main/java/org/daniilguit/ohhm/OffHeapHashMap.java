package org.daniilguit.ohhm;

import org.daniilguit.ohhm.util.BufferUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by Daniil Gitelson on 23.04.15.
 */
public class OffHeapHashMap {
    public static final double MAX_TABLE_DENSITY = 0.9;
    private final OffHeapHashMapPartition[] partitions;

    public OffHeapHashMap(OffHeapHashMapConfig config) {
        partitions = new OffHeapHashMapPartition[config.partitions];

        for (int i = 0; i < partitions.length; i++) {
            float variateDensity = config.tableDensity + i * config.tableDensity / 100 / partitions.length;
            partitions[i] = new OffHeapHashMapPartition(config, Math.min(variateDensity, MAX_TABLE_DENSITY));
        }
    }

    public OffHeapHashMapPartition.Entry entry(ByteBuffer key) {
        int hash = key.hashCode();
        return partitions[Math.abs(hash) % partitions.length].entry(key, hash);
    }

    private class OffHeapHashMapPartition implements CompactionCallback {
        private volatile HashTable hashTable;
        private final CompactingBuffer compactingBuffer;
        private final double density;
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        public OffHeapHashMapPartition(OffHeapHashMapConfig config, double density) {
            hashTable = new HashTable(config.initialSize / config.partitions);
            compactingBuffer = new CompactingBuffer(config.bucketSize);
            this.density = density;
        }

        @Override
        public void updated(ByteBuffer data, long oldLocation, long newLocation) {
            int hashCode = data.getInt();
            hashTable.update(hashCode, oldLocation, newLocation);
        }

        public class Entry implements HashTable.LookupPredicate {
            final int hash;
            final ByteBuffer key;

            HashTable hashTable;
            int offset;
            long location = -1;

            public Entry(int hash, ByteBuffer key) {
                this.hash = hash;
                this.key = key;
            }

            private long getLocation() {
                HashTable hashTable = OffHeapHashMapPartition.this.hashTable;
                if (location < 0 || this.hashTable == null || this.hashTable != hashTable) {
                    this.hashTable = hashTable;
                    offset = hashTable.find(hash, this);
                    if (offset >= 0) {
                        location = hashTable.getLocationAtOffset(offset);
                    } else {
                        location = -1;
                    }
                }
                return location;
            }

            public ByteBuffer get() {
                getLocation();
                if (location < 0) {
                    return null;
                }
                ByteBuffer entry;
                do {
                    entry = compactingBuffer.compute(
                            hashTable.getLocationAtOffset(offset),
                            buffer -> ByteBuffer.allocate(buffer.remaining()).put(buffer)
                    );
                } while (entry == null);
                entry.position(0);
                int keySize = BufferUtils.readPackedInt(entry);
                BufferUtils.skip(entry, keySize);
                return entry;
            }

            public void put(ByteBuffer value) {
                getLocation();
                ByteBuffer entry = ByteBuffer.allocate(key.remaining() + value.remaining() + BufferUtils.packedSize(key.remaining()));
                BufferUtils.writePackedLong(entry, key.remaining());
                BufferUtils.writeBuffer(entry, key);
                BufferUtils.writeBuffer(entry, value);
                entry.position(0);
                long oldLocation = this.location;
                this.location = compactingBuffer.append(entry, oldLocation);
                if (oldLocation < 0) {
                    if (!hashTable.insert(hash, location) || hashTable.density() > density) {
                        HashTable newTable = new HashTable(hashTable.size() * 3 / 2);
                        newTable.insert(hashTable);
                        OffHeapHashMapPartition.this.hashTable = hashTable = newTable;
                    }
                } else {
                    hashTable.update(hash, oldLocation, location);
                }
            }

            @Override
            public boolean check(long location) {
                return compactingBuffer.<Boolean>compute(location, buffer -> {
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
