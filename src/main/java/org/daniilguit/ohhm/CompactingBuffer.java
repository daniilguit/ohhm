package org.daniilguit.ohhm;

import org.daniilguit.ohhm.util.BufferUtils;
import org.daniilguit.ohhm.util.OffHeapBitSet;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Created by Daniil Gitelson on 22.04.15.
 */
public class CompactingBuffer {
    private final CompactionCallback compactionCallback;
    private final ExecutorService gcExecutor;
    private final double compactionThreshold = 0.8;
    private final int bucketSize;

    private final AtomicInteger bucketsIdGenerator = new AtomicInteger();
    private final ConcurrentHashMap<Integer, Bucket> buckets = new ConcurrentHashMap<>();

    private final AtomicBoolean compactionState = new AtomicBoolean();
    private final Lock appenderLock = new ReentrantLock();
    private volatile BucketAppender bucketAppender;

    public CompactingBuffer(int bucketSize, CompactionCallback compactionCallback) {
        this(bucketSize, compactionCallback, Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() / 2, r -> {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("CompactionGarbageCollector");
            thread.setPriority(Thread.MIN_PRIORITY);
            return thread;
        }));
    }

    public CompactingBuffer(int bucketSize, CompactionCallback compactionCallback, ExecutorService gcExecutor) {
        this.bucketSize = bucketSize;
        this.compactionCallback = compactionCallback;
        this.gcExecutor = gcExecutor;
        chooseNewAppendingBuffer();
    }

    public long append(ByteBuffer data) {
        assert data.remaining() > 0;
        int entrySize = entrySize(data.remaining());
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
        return bucketAppender.put(offset, data, entrySize);
    }

    private BucketAppender chooseNewAppendingBuffer() {
        if (bucketAppender != null) {
            bucketAppender.bucket.active.set(false);
        }
        bucketAppender = new BucketAppender(allocateBucket(nextBucketId(), true));
        return bucketAppender;
    }

    private int nextBucketId() {
        int id;
        do {
            id = bucketsIdGenerator.getAndIncrement() * 2;
        } while (buckets.containsKey(id));
        return id;
    }

    private int entrySize(int dataSize) {
        return BufferUtils.packedLongSize(dataSize) + dataSize;
    }

    private int offsetForLocation(long location) {
        return (int) (location);
    }

    private Bucket bucketForLocation(long location) {
        return buckets.get((int) (location >> 32));
    }

    private Bucket allocateBucket(int index, boolean active) {
        Bucket bucket = new Bucket(ByteBuffer.allocateDirect(bucketSize), index, active);
        assert !buckets.containsKey(bucket.index);
        buckets.put(bucket.index, bucket);
        return bucket;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public void access(LongSupplier locationSupplier, Consumer<ByteBuffer> consumer) {
        for (int count = 0; count < 10; count++) {
            if (this.<Boolean>compute(locationSupplier.getAsLong(), buffer -> {
                consumer.accept(buffer);
                return true;
            }) == Boolean.TRUE) {
                return;
            }
        }
        throw new NoSuchElementException();
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


    public void compact() {
        if (compactionState.compareAndSet(false, true)) {
            try {
                doCompact();
            } finally {
                compactionState.set(false);
            }
        }
    }

    private void doCompact() {

        new GarbageCollector(inactiveBuckets()).run();

        List<Bucket> buckets = inactiveBuckets();
        if (memoryEfficiency(buckets) < compactionThreshold) {
            new FullCompaction(buckets).run();
        }
    }

    private List<Bucket> inactiveBuckets() {
        return buckets.values().stream().filter(Bucket::isFinished).collect(Collectors.toList());
    }

    public long memoryUsage() {
        return buckets.size() * bucketSize;
    }

    private static double memoryEfficiency(Collection<Bucket> buckets) {
        return 1. * buckets.stream().mapToInt(Bucket::used).reduce(0, Integer::sum) / buckets.stream().mapToInt(b -> b.buffer.capacity()).reduce(0, Integer::sum);
    }

    public double memoryEfficiency() {
        return memoryEfficiency(buckets.values());
    }

    private class Bucket {
        public final ByteBuffer buffer;
        public final int index;
        public final AtomicBoolean active;
        public final AtomicInteger written = new AtomicInteger();
        public final AtomicInteger limit = new AtomicInteger();

        public Bucket(ByteBuffer buffer, int index, boolean active) {
            this.buffer = buffer;
            this.index = index;
            this.active = new AtomicBoolean(active);
        }

        public boolean isFinished() {
            return written.get() == limit.get() && !active.get();
        }

        public int used() {
            return limit.get();
        }

        public ByteBuffer bufferDuplicate() {
            return buffer.duplicate();
        }

        public long offsetToLocation(int offset) {
            return ((long) index << 32L) + offset;
        }

        public double efficiency() {
            return 1. * used() / buffer.capacity();
        }

        public double efficiency(int garbageSize) {
            return 1. * (used() - garbageSize) / used();
        }

        public void remove() {
            buckets.remove(index, this);
        }
    }

    private static class BucketAppender {
        final Bucket bucket;
        final AtomicInteger offset = new AtomicInteger();

        public BucketAppender(Bucket bucket) {
            this.bucket = bucket;
            offset.set(bucket.limit.get());
        }

        public int requestOffset(int forSize) {
            int gotOffset = offset.addAndGet(forSize);
            if (gotOffset <= bucket.buffer.capacity()) {
                bucket.limit.addAndGet(forSize);
                return gotOffset - forSize;
            }
            return -1;
        }

        public long put(int offset, ByteBuffer data, int entrySize) {
            ByteBuffer dest = bucket.bufferDuplicate();
            dest.position(offset);
            BufferUtils.writePackedLong(dest, data.remaining());
            BufferUtils.writeBuffer(dest, data);
            assert dest.position() - offset == entrySize;
            bucket.written.addAndGet(entrySize);
            return bucket.offsetToLocation(offset);
        }
    }

    private class GarbageCollector implements Runnable {
        private final Collection<Bucket> bucketsToCompact;

        public GarbageCollector(Collection<Bucket> buckets) {
            bucketsToCompact = buckets;
        }

        @Override
        public void run() {
            CompletableFuture.allOf(
                    bucketsToCompact.stream()
                            .map(bucket -> CompletableFuture.runAsync(
                                            () -> compactBucket(bucket),
                                            gcExecutor)
                            )
                            .<CompletableFuture<Void>>toArray(CompletableFuture[]::new)
            ).join();
        }

        private void compactBucket(Bucket bucket) {
            OffHeapBitSet garbageFlags = new OffHeapBitSet(bucket.used());
            int garbageSize = calculateGarbageInfo(bucket, garbageFlags);

            if (garbageSize == bucket.used()) {
                // All entries in bucket are garbage
                bucket.remove();
            } else {
                if (bucket.efficiency(garbageSize) < compactionThreshold) {
                    doCompactBucket(bucket, garbageFlags);
                }
            }
        }

        private int calculateGarbageInfo(Bucket bucket, OffHeapBitSet garbageFlag) {
            ByteBuffer bucketBuffer = bucket.bufferDuplicate();
            bucketBuffer.limit(bucket.used());
            ByteBuffer dataBuffer = bucket.bufferDuplicate();
            int garbageSize = 0;
            while (bucketBuffer.remaining() > 0) {
                int position = bucketBuffer.position();
                int size = BufferUtils.readPackedInt(bucketBuffer);
                int dataPosition = bucketBuffer.position();
                long location = bucket.offsetToLocation(position);
                BufferUtils.skip(bucketBuffer, size);
                if (compactionCallback.isGarbage(BufferUtils.locate(dataBuffer, dataPosition, size), location)) {
                    garbageFlag.set(position);
                    garbageSize += bucketBuffer.position() - position;
                }
            }
            return garbageSize;
        }

        private Bucket doCompactBucket(Bucket bucket, OffHeapBitSet garbageFlags) {
            ByteBuffer sourceBuffer = bucket.bufferDuplicate();
            ByteBuffer tempBuffer = bucket.bufferDuplicate();

            sourceBuffer.limit(bucket.limit.get());

            Bucket newBucket = allocateBucket(alternateIndex(bucket), false);
            ByteBuffer destBuffer = newBucket.buffer.duplicate();

            while (sourceBuffer.remaining() > 0) {
                int position = sourceBuffer.position();
                int size = BufferUtils.readPackedInt(sourceBuffer);
                BufferUtils.skip(sourceBuffer, size);
                if (!garbageFlags.test(position)) {
                    long newLocation = newBucket.offsetToLocation(destBuffer.position());
                    destBuffer.put(BufferUtils.locate(tempBuffer, position, sourceBuffer.position() - position));
                    compactionCallback.updated(
                            BufferUtils.locate(tempBuffer, sourceBuffer.position() - size, size),
                            bucket.offsetToLocation(position),
                            newLocation
                    );
                }
            }

            newBucket.written.set(destBuffer.position());
            newBucket.limit.set(destBuffer.position());

            bucket.remove();

            return newBucket;
        }
    }

    private class FullCompaction implements Runnable {
        private final Collection<Bucket> bucketsToCompact;

        private Bucket destinationBucket;
        private ByteBuffer destinationBuffer;

        private FullCompaction(Collection<Bucket> bucketsToCompact) {
            this.bucketsToCompact = bucketsToCompact;
        }


        public void run() {
            bucketsToCompact.stream()
                    // Do not compact buckets that are almost full for performance reasons
                    .filter(bucket -> bucket.efficiency() < compactionThreshold)
                    // Heuristics: compact buckets from less used to more used
                    .sorted(Comparator.comparingInt(Bucket::used))
                    .forEachOrdered(this::appendBucket);
        }

        private void appendBucket(Bucket bucket) {
            if (destinationBucket == null) {
                setDestinationBucket(bucket);
            } else {
                if (destinationBuffer.remaining() >= bucket.used()) {
                    appendBucket(bucket, BufferUtils.locate(bucket.bufferDuplicate(), 0, bucket.used()));
                    bucket.remove();
                } else {
                    ByteBuffer sourceBuffer = bucket.bufferDuplicate();
                    sourceBuffer.limit(bucket.used());
                    int splitAround = splitAround(sourceBuffer, destinationBuffer.remaining());
                    if (splitAround == 0) {
                        // No entries fits inside destination bucket, move destination bucket pointer to next bucket
                        setDestinationBucket(bucket);
                    } else {
                        // Split bucket into two parts, first one copy to destination buffer
                        // Next one clone and set as new destination bucket
                        appendBucket(bucket, BufferUtils.locate(sourceBuffer, 0, splitAround));
                        setDestinationBucket(allocateBucket(alternateIndex(bucket), false));
                        appendBucket(bucket, BufferUtils.locate(sourceBuffer, splitAround, bucket.used() - splitAround));
                        bucket.remove();
                    }
                }
            }
        }

        private void setDestinationBucket(Bucket bucket) {
            destinationBucket = bucket;
            destinationBuffer = bucket.bufferDuplicate();
            destinationBuffer.position(bucket.used());
        }

        private int splitAround(ByteBuffer buffer, int position) {
            int lastBufferPosition = buffer.position();
            while (buffer.position() < position) {
                lastBufferPosition = buffer.position();
                int size = BufferUtils.readPackedInt(buffer);
                BufferUtils.skip(buffer, size);
            }
            buffer.position(0);
            return lastBufferPosition;
        }

        private void appendBucket(Bucket sourceBucket, ByteBuffer sourceBuffer) {
            destinationBuffer.put(sourceBuffer.duplicate());

            remapLocations(sourceBucket, sourceBuffer.duplicate());
            destinationBucket.written.set(destinationBuffer.position());
            destinationBucket.limit.set(destinationBuffer.position());
        }

        private void remapLocations(Bucket sourceBucket, ByteBuffer sourceBuffer) {
            int destinationOffset = destinationBucket.used();
            ByteBuffer tempBuffer = sourceBuffer.duplicate();
            int originalSourcePosition = sourceBuffer.position();
            while (sourceBuffer.remaining() > 0) {
                int positionInSource = sourceBuffer.position();
                int size = BufferUtils.readPackedInt(sourceBuffer);
                compactionCallback.updated(
                        BufferUtils.locate(tempBuffer, sourceBuffer.position(), size),
                        sourceBucket.offsetToLocation(positionInSource),
                        destinationBucket.offsetToLocation(destinationOffset + positionInSource - originalSourcePosition)
                );
                BufferUtils.skip(sourceBuffer, size);
            }
        }
    }

    private int alternateIndex(Bucket bucket) {
        // Invert last bit
//        return bucket.index ^ 1;
        return nextBucketId();
    }
}
