package org.daniilguit.ohhm;

import org.daniilguit.ohhm.util.BufferUtils;
import org.daniilguit.ohhm.util.OffHeapBitSet;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by Daniil Gitelson on 22.04.15.
 */
public class CompactingBuffer {

    private static final double COMPACTION_THRESHOLD = 0.8;
    private static final Comparator<Bucket> BUCKET_COMPARATOR = (o1, o2) -> Integer.compare(o1.available(), o2.available());

    private final int bucketSize;
    private final AtomicLong idGenerator = new AtomicLong();
    private final AtomicInteger bucketsIdGenerator = new AtomicInteger();
    private final ConcurrentHashMap<Integer, Bucket> buckets = new ConcurrentHashMap<>();

    private final AtomicBoolean compactionState = new AtomicBoolean();
    private final Lock appenderLock = new ReentrantLock();
    private volatile BucketAppender bucketAppender;

    public CompactingBuffer(int bucketSize) {
        this.bucketSize = bucketSize;
        chooseNewAppendingBuffer();
    }

    public long append(ByteBuffer data) {
        return append(data, -1);
    }

    public long append(ByteBuffer data, long oldLocation) {
        assert data.remaining() > 0;
        long id = oldLocation >= 0 ? getIdFor(oldLocation) : nextId();
        int entrySize = entrySize(data.remaining(), id);
        int offset;
        BucketAppender bucketAppender = this.bucketAppender;
        do {
            offset = bucketAppender.requestOffset(entrySize);
            if (offset < 0) {
                appenderLock.lock();
                try {
                    offset = (bucketAppender = this.bucketAppender).requestOffset(entrySize);
                    if (offset < 0) {
                        bucketAppender = chooseNewAppendingBuffer();
                    }
                } finally {
                    appenderLock.unlock();
                }
            }
        } while (offset < 0);
        return bucketAppender.put(offset, data, id, oldLocation, entrySize);
    }

    private BucketAppender chooseNewAppendingBuffer() {
        if (bucketAppender != null) {
            bucketAppender.bucket.active.set(false);
        }
        Bucket newBucket = allocateBufferState(bucketsIdGenerator.getAndIncrement() * 2);
        buckets.put(newBucket.index, newBucket);
        bucketAppender = new BucketAppender(newBucket);
        return bucketAppender;
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

    private int offsetForLocation(long location) {
        return (int) (location % bucketSize);
    }

    private Bucket bucketForLocation(long location) {
        return buckets.get((int) (location / bucketSize));
    }

    private Bucket allocateBufferState(int index) {
        return new Bucket(ByteBuffer.allocateDirect(bucketSize), index);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public void access(LocationSupplier locationSupplier, Consumer<ByteBuffer> consumer) {

        while (this.<Boolean>compute(locationSupplier.location(), buffer -> {
            consumer.accept(buffer);
            return true;
        })) ;
    }

    public <T> T compute(long location, Function<ByteBuffer, T> consumer) {
        Bucket bucket;
        if ((bucket = bucketForLocation(location)) == null) {
            return null;
        }
        ByteBuffer temp = bucket.bufferDuplicate();
        int offset = offsetForLocation(location);
        temp.position(offset);
        int size = BufferUtils.readPackedInt(temp);
        temp.limit(temp.position() + size);
        return consumer.apply(temp);
    }


    private final AtomicInteger comapctCounter = new AtomicInteger();

    public void compact(CompactionCallback callback) {
        if (compactionState.compareAndSet(false, true)) {
            try {
                new Compactor(callback).run();
            } finally {
                compactionState.set(false);
            }
        }
    }

    public long memoryUsage() {
        return buckets.size() * bucketSize;
    }

    public long memoryOverhead() {
        return buckets.values().stream().mapToInt(Bucket::available).reduce(0, Integer::sum);
    }

    public double memoryEfficiency() {
        return 1 - 1.0 * memoryOverhead() / memoryUsage();
    }

    interface LocationSupplier {
        long location();
    }

    private class Bucket {
        final ByteBuffer buffer;
        final int index;
        final AtomicBoolean active = new AtomicBoolean(true);
        final AtomicInteger written = new AtomicInteger();
        final AtomicInteger limit = new AtomicInteger();
        final AtomicInteger count = new AtomicInteger();
        final AtomicInteger updates = new AtomicInteger();

        public Bucket(ByteBuffer buffer, int index) {
            this.buffer = buffer;
            this.index = index;
        }

        public boolean isFinished() {
            return written.get() == limit.get() && !active.get();
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

        long put(int offset, ByteBuffer data, long id, long oldLocation, int entrySize) {
            ByteBuffer dest = bucket.bufferDuplicate();
            dest.position(offset);
            BufferUtils.writePackedLong(dest, data.remaining());
            BufferUtils.writeBuffer(dest, data);
            BufferUtils.writePackedLong(dest, id);
            assert dest.position() - offset == entrySize;
            if (oldLocation >= 0) {
                Bucket oldBucket = bucketForLocation(oldLocation);
                oldBucket.updates.incrementAndGet();
                if (oldBucket != bucketAppender.bucket) {
                    bucket.count.incrementAndGet();
                }
            } else {
                bucket.count.incrementAndGet();
            }
            if (id != getIdFor(bucket.offsetToLocation(offset))) {
                getIdFor(bucket.offsetToLocation(offset));
            }
            bucket.written.addAndGet(entrySize);
            return bucket.offsetToLocation(offset);
        }
    }

    private class Compactor {
        private final CompactionCallback callback;
        private final OffHeapBitSet visited;
        private final IntBuffer bucketOffsets;
        private final List<Bucket> bucketsSnapshot;

        public Compactor(CompactionCallback callback) {
            this.callback = callback;
            List<Bucket> bucketsSnapshot = CompactingBuffer.this.buckets.values().stream().sorted((b1, b2) -> -Integer.compare(b1.index, b2.index)).collect(Collectors.toList());
            // There could not be elements added but that fits into on bucket
            long maxId = idGenerator.get() + bucketSize;
            for (int i = 0; i < bucketsSnapshot.size(); i++) {
                if (!bucketsSnapshot.get(i).isFinished()) {
                    bucketsSnapshot = bucketsSnapshot.subList(0, i);
                }
            }
            this.bucketsSnapshot = bucketsSnapshot;
            visited = new OffHeapBitSet(maxId);
            bucketOffsets = ByteBuffer.allocateDirect((bucketSize / 3 + 1) * Integer.BYTES).asIntBuffer();
        }

        public void run() {
            BucketAppender bucketAppender = CompactingBuffer.this.bucketAppender;
            for (Bucket bucket : bucketsSnapshot) {
                ByteBuffer bucketBuffer = bucket.bufferDuplicate();
                if (bucketAppender.bucket == bucket) {
                    markGarbage(bucket, bucketBuffer);
                } else {
                    collectOffsets(bucket, bucketBuffer);
                    compactBucket(bucket, bucketBuffer);
                }
            }
            //            mergeBuckets();
        }

        /**
         * Merge buckets for compaction (todo)
         */
        private void mergeBuckets() {
        }

        private void mergeBuckets(int startMerge, int endMerge) {
//            Bucket destBucket = buckets[startMerge];
//            ByteBuffer destBuffer = destBucket.bufferDuplicate();
//            destBuffer.position(destBucket.used());
//            for (int i = startMerge + 1; i <= endMerge; i++) {
//                appendBucket(destBucket, destBuffer, buckets[i]);
//            }
//            destBucket.limit.set(destBuffer.position());
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
//                callback.updated(dataBuffer,bucket. , destBucket.offsetToLocation(offset + position));
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
            Bucket newBucket = allocateBufferState(alternateIndex(bucket));
            newBucket.active.set(false);
            buckets.put(newBucket.index, newBucket);
            int limit = bucket.limit.get();
            sourceBuffer.limit(limit);
            ByteBuffer destBuffer = newBucket.buffer.duplicate();
            ByteBuffer dataBuffer = sourceBuffer.duplicate();
            int copied = 0;
            int removed = 0;
            for (int i = bucketOffsets.limit() - 1; i >= 0; i--) {
                int position = bucketOffsets.get(i);

                sourceBuffer.position(position);

                int dataSize = BufferUtils.readPackedInt(sourceBuffer);
                int dataOffset = sourceBuffer.position();
                BufferUtils.skip(sourceBuffer, dataSize);
                long id = BufferUtils.readPackedLong(sourceBuffer);
                int endPosition = sourceBuffer.position();

                if (!visited.test(id)) {
                    visited.set(id);

                    int destPosition = destBuffer.position();
                    BufferUtils.locate(dataBuffer, position, endPosition - position);
                    destBuffer.put(dataBuffer);

                    BufferUtils.locate(dataBuffer, dataOffset, dataSize);
                    callback.updated(dataBuffer, bucket.offsetToLocation(position), newBucket.offsetToLocation(destPosition));

                    copied++;
                } else {
                    removed++;
                }
            }
            if (destBuffer.position() == 0) {
                bucketsSnapshot.remove(bucket.index);
            } else {
                newBucket.limit.set(destBuffer.position());
                newBucket.count.set(copied);
                newBucket.updates.set(0);

                buckets.remove(bucket.index, bucket);
            }
        }

        private int alternateIndex(Bucket bucket) {
            return (bucket.index & ~1) + (1 - (bucket.index & 1));
        }
    }
}
