package org.daniilguit.ohhm;

import org.daniilguit.ohhm.util.BufferUtils;
import org.daniilguit.ohhm.util.OffHeapBitSet;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Created by Daniil Gitelson on 22.04.15.
 */
public class CompactingBuffer {
    private static final int MEGABYTE = 1 << 20;

    private static final double COMPACTION_THRESHOLD = 0.8;
    private static final Comparator<Bucket> BUCKET_COMPARATOR = (o1, o2) -> Integer.compare(o1.available(), o2.available());

    private final int bucketSize;
    private final AtomicLong idGenerator = new AtomicLong();

    private volatile Bucket[] buckets;
    private volatile Bucket[] sortedBuffers;
    private volatile BucketAppender bucketAppender;

    public CompactingBuffer(int bucketSize) {
        this.bucketSize = bucketSize;
        sortedBuffers = buckets = new Bucket[]{allocateBufferState(0)};
        bucketAppender = new BucketAppender(buckets[0]);
    }

    public long append(ByteBuffer data) {
        return append(data, -1);
    }

    public long append(ByteBuffer data, long oldLocation) {
        try {
            long id = oldLocation >= 0 ? getIdFor(oldLocation) : nextId();
            int entrySize = entrySize(data.remaining(), id);
            int offset;
            BucketAppender bucketAppender;
            do {
                offset = (bucketAppender = this.bucketAppender).requestOffset(entrySize);
                if (offset < 0) {
                    chooseNewAppendingBuffer(entrySize);
                }
            } while (offset < 0);
            return bucketAppender.put(offset, data, id, oldLocation);
        } finally {
        }
    }

    private long nextId() {
        return idGenerator.incrementAndGet();
    }

    private long getIdFor(long location) {
        ByteBuffer buffer = bucketForLocation(location).bufferDuplicate();
        buffer.position(offsetForLocation(location));
        int size = BufferUtils.readPackedInt(buffer);
        BufferUtils.skip(buffer, size);
        return BufferUtils.readPackedLong(buffer);
    }

    private int entrySize(int dataSize, long oldLocation) {
        return BufferUtils.packedSize(dataSize) + BufferUtils.packedSize(oldLocation) + dataSize;
    }

    private void chooseNewAppendingBuffer(int size) {
        try {
            int bucketIndex = bucketAppender.bucket.sortedIndex;

            while (bucketIndex >= 0 && sortedBuffers[bucketIndex].available() < size) {
                bucketIndex--;
            }
            Bucket leastUsedBucket;
            if (bucketIndex >= 0) {
                leastUsedBucket = sortedBuffers[bucketIndex];
            } else {
                leastUsedBucket = allocateBufferState(buckets.length);
                buckets = Arrays.copyOf(buckets, buckets.length + 1);
                sortedBuffers = Arrays.copyOf(sortedBuffers, sortedBuffers.length + 1);
                buckets[buckets.length - 1] = leastUsedBucket;
                sortedBuffers[buckets.length - 1] = leastUsedBucket;
            }
            bucketAppender = new BucketAppender(leastUsedBucket);
        } finally {
        }
    }

    private int offsetForLocation(long location) {
        return (int) (location % bucketSize);
    }

    private Bucket bucketForLocation(long location) {
        return buckets[((int) (location / bucketSize))];
    }

    private Bucket allocateBufferState(int index) {
        return new Bucket(ByteBuffer.allocateDirect(bucketSize), index);
    }

    public <T> T evaluate(long location, Function<ByteBuffer, T> function) {
        Bucket bucket = bucketForLocation(location);
        ByteBuffer temp = bucket.bufferDuplicate();
        int offset = offsetForLocation(location);
        temp.position(offset);
        int size = BufferUtils.readPackedInt(temp);
        temp.limit(temp.position() + size);
        return function.apply(temp);
    }

    public void compact(CompactionCallback callback) {
        try {
            new Compactor(buckets, callback).run();
        } finally {
        }
    }

    public long memoryUsage() {
        return buckets.length * bucketSize;
    }

    private class Bucket {
        final ByteBuffer buffer;
        final int index;
        final AtomicInteger limit = new AtomicInteger();
        final AtomicInteger count = new AtomicInteger();
        final AtomicInteger updates = new AtomicInteger();
        volatile int sortedIndex;

        public Bucket(ByteBuffer buffer, int index) {
            this.buffer = buffer;
            this.index = index;
            this.sortedIndex = index;
        }

        public boolean needsCompaction() {
            return count.get() * COMPACTION_THRESHOLD < updates.get();
        }

        public int available() {
            return bucketSize - limit.get();
        }

        public int used() {
            return limit.get();
        }

        public ByteBuffer bufferDuplicate() {
            return buffer.duplicate();
        }

        long offsetToLocation(int offset) {
            return (long) index * bucketSize + offset;
        }
    }

    private class BucketAppender {
        final Bucket bucket;
        final AtomicInteger offset = new AtomicInteger();

        private BucketAppender(Bucket bucket) {
            this.bucket = bucket;
            offset.set(bucket.limit.get());
        }

        int requestOffset(int forSize) {
            int gotOffset = offset.addAndGet(forSize);
            if (gotOffset <= bucketSize) {
                bucket.limit.addAndGet(forSize);
                return gotOffset - forSize;
            }
            return -1;
        }

        long put(int offset, ByteBuffer data, long id, long oldLocation) {
            ByteBuffer dest = bucket.bufferDuplicate();
            dest.position(offset);
            BufferUtils.writePackedLong(dest, data.remaining());
            BufferUtils.writeBuffer(dest, data);
            BufferUtils.writePackedLong(dest, id);

            if (oldLocation >= 0) {
                Bucket oldBucket = bucketForLocation(oldLocation);
                oldBucket.updates.incrementAndGet();
                if (oldBucket != bucketAppender.bucket) {
                    bucket.count.incrementAndGet();
                }
            } else {
                bucket.count.incrementAndGet();
            }
            return bucket.offsetToLocation(offset);
        }
    }

    private class Compactor {
        private final Bucket[] buckets;
        private final CompactionCallback callback;
        private final OffHeapBitSet visited;
        private final IntBuffer bucketOffsets;

        public Compactor(Bucket[] buckets, CompactionCallback callback) {
            this.buckets = buckets;
            this.callback = callback;

            long maxId = idGenerator.get();
            visited = new OffHeapBitSet(maxId);
            bucketOffsets = ByteBuffer.allocateDirect((bucketSize / 3 + 1) * Integer.BYTES).asIntBuffer();
        }

        public void run() {
            for (int i = buckets.length - 1; i >= 0; i--) {
                Bucket bucket = buckets[i];
                ByteBuffer bucketBuffer = bucket.bufferDuplicate();
                collectOffsets(bucket, bucketBuffer);
                compactBucket(bucket, bucketBuffer);
            }
            sortBuffers();
//            mergeBuckets();
        }

        /**
         * Merge buckets for compaction (todo)
         */
        private void mergeBuckets() {
            int endMerge = sortedBuffers.length - 2;
            int startMerge = endMerge;
            int totalSize = 0;
            while (startMerge >= 0 && totalSize < bucketSize) {
                totalSize += sortedBuffers[startMerge].used();
                startMerge--;
            }
            if (totalSize > bucketSize) {
                startMerge++;
            }
            if (startMerge < endMerge) {
                mergeBuckets(startMerge, endMerge);
            }
        }

        private void mergeBuckets(int startMerge, int endMerge) {
            Bucket destBucket = buckets[startMerge];
            ByteBuffer destBuffer = destBucket.bufferDuplicate();
            destBuffer.position(destBucket.used());
            for (int i = startMerge + 1; i <= endMerge; i++) {
                appendBucket(destBucket, destBuffer, buckets[i]);
            }
            destBucket.limit.set(destBuffer.position());
        }

        private void appendBucket(Bucket destBucket, ByteBuffer destBuffer, Bucket bucket) {
            ByteBuffer sourceBuffer = bucket.bufferDuplicate();
            ByteBuffer dataBuffer = bucket.bufferDuplicate();
            int limit = bucket.used();
            sourceBuffer.limit(limit);
            int offset = destBuffer.position();
            destBuffer.put(sourceBuffer);
            sourceBuffer.position(0);
            while (sourceBuffer.remaining() > 0) {
                int position = sourceBuffer.position();
                int size = BufferUtils.readPackedInt(sourceBuffer);
                dataBuffer.limit(sourceBuffer.position() + size);
                dataBuffer.position(sourceBuffer.position());
                BufferUtils.skip(sourceBuffer, size);
                BufferUtils.readPackedLong(sourceBuffer);
                callback.updated(dataBuffer, destBucket.offsetToLocation(offset + position));
            }
        }

        private void markGarbage(Bucket bucket, ByteBuffer bucketBuffer) {
            bucketBuffer.position(0);
            int limit = bucket.limit.get();
            while (bucketBuffer.position() < limit) {
                int size = BufferUtils.readPackedInt(bucketBuffer);
                BufferUtils.skip(bucketBuffer, size);
                long id = BufferUtils.readPackedLong(bucketBuffer);


                if (!visited.test(id)) {
                    visited.set(id);
                }
            }
        }

        private void collectOffsets(Bucket bucket, ByteBuffer bucketBuffer) {
            bucketBuffer.position(0);
            bucketBuffer.limit(bucket.limit.get());
            bucketOffsets.position(0);
            bucketOffsets.limit(bucketOffsets.capacity());
            while (bucketBuffer.remaining() > 0) {
                int position = bucketBuffer.position();
                int size = BufferUtils.readPackedInt(bucketBuffer);
                BufferUtils.skip(bucketBuffer, size);
                long id = BufferUtils.readPackedLong(bucketBuffer);
                if (!visited.test(id)) {
                    bucketOffsets.put(position);
                }
            }
            bucketOffsets.limit(bucketOffsets.position());
            bucketOffsets.position(0);
        }

        private void compactBucket(Bucket bucket, ByteBuffer sourceBuffer) {
            Bucket newBucket = allocateBufferState(bucket.index);
            int limit = bucket.limit.get();
            sourceBuffer.limit(limit);
            ByteBuffer destBuffer = newBucket.buffer.duplicate();
            int copied = 0;
            int removed = 0;
            for (int i = bucketOffsets.limit() - 1; i >= 0; i--) {
                int position = bucketOffsets.get(i);

                sourceBuffer.limit(sourceBuffer.capacity());
                sourceBuffer.position(position);

                int dataSize = BufferUtils.readPackedInt(sourceBuffer);
                int dataOffset = sourceBuffer.position();
                BufferUtils.skip(sourceBuffer, dataSize);
                long id = BufferUtils.readPackedLong(sourceBuffer);
                int entryEnd = sourceBuffer.position();

                if (!visited.test(id)) {
                    visited.set(id);

                    sourceBuffer.position(dataOffset);
                    sourceBuffer.limit(dataOffset + dataSize);
                    callback.updated(sourceBuffer, bucket.offsetToLocation(destBuffer.position()));

                    sourceBuffer.limit(entryEnd);
                    sourceBuffer.position(position);
                    destBuffer.put(sourceBuffer);

                    copied++;
                } else {
                    removed++;
                }
            }
            newBucket.limit.set(destBuffer.position());
            newBucket.count.set(copied);
            newBucket.updates.set(0);
            newBucket.sortedIndex = bucket.sortedIndex;

            buckets[bucket.index] = newBucket;
            sortedBuffers[bucket.sortedIndex] = newBucket;
        }

        private void sortBuffers() {
            try {
                Arrays.sort(sortedBuffers, BUCKET_COMPARATOR);
                for (int i = 0; i < sortedBuffers.length; i++) {
                    sortedBuffers[i].sortedIndex = i;
                }
            } finally {
            }
        }
    }
}
