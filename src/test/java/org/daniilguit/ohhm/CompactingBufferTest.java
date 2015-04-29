package org.daniilguit.ohhm;

import org.daniilguit.ohhm.util.BufferUtils;
import org.daniilguit.ohhm.util.LockManager;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Daniil Gitelson on 23.04.15.
 */
public class CompactingBufferTest {
    @Test
    public void testAdd() throws Exception {
        Map<Integer, Integer> expected = new ConcurrentHashMap<>();
        AtomicLongArray locations = new AtomicLongArray(10000);
        CompactingBuffer buckets = new CompactingBuffer(100, new CompactionCallback(locations), Executors.newSingleThreadExecutor());

        for (int j = 0; j < 3; j++) {
            for (int i = j * locations.length() / 3; i < locations.length(); i++) {
                ByteBuffer entryBuffer = bufferForEntry(i, i + j);
                expected.put(i, i + j);
                locations.set(i, buckets.append(entryBuffer));
            }
        }

        checkBuckets(expected, locations, buckets);
    }

    @Test
    public void testCompact() throws Exception {
        Map<Integer, Integer> expected = new ConcurrentHashMap<>();
        AtomicLongArray locations = new AtomicLongArray(10000);
        CompactingBuffer buckets = new CompactingBuffer(100, new CompactionCallback(locations), Executors.newSingleThreadExecutor());

        Random r = new Random(0);
        for (int repeat = 0; repeat < 200; repeat++) {
            for (int i = 0; i < locations.length() / 100; i += 3) {
                int index = r.nextInt(locations.length());
                locations.set(index, buckets.append(bufferForEntry(index, index + repeat)));
                expected.put(index, index + repeat);
            }
            buckets.compact();
            checkBuckets(expected, locations, buckets);
        }
        buckets.compact();
        checkMemoryUsage(expected, buckets);
    }

    @Test
    public void testCompactRandom() throws Exception {
        Map<Integer, Integer> expected = new ConcurrentHashMap<>();
        AtomicLongArray locations = new AtomicLongArray(1000);
        CompactingBuffer buckets = new CompactingBuffer(20, new CompactionCallback(locations), Executors.newSingleThreadExecutor());

        Random random = new Random(0);
        for (int r = 0; r < 1000; r++) {
            for (int i = 0; i < locations.length(); i++) {
                int key = random.nextInt(locations.length());
                int value = random.nextInt();

                locations.set(key, buckets.append(bufferForEntry(key, value)));
                expected.put(key, value);
            }
            buckets.compact();
            checkBuckets(expected, locations, buckets);
        }
        buckets.compact();
        checkMemoryUsage(expected, buckets);
    }

    private void checkMemoryUsage(Map<Integer, Integer> expected, CompactingBuffer buckets) {
        int idealSize = expected.size() * (bufferForEntry(0, 0).remaining() + BufferUtils.packedLongSize(bufferForEntry(0, 0).remaining()));
        System.out.println("Memory used: " + buckets.memoryUsage());
        System.out.println("Theoretical memory should be used: " + idealSize);
        System.out.println("Memory efficiency: " + 1.0 * idealSize);
        assertTrue(buckets.memoryUsage() * 0.7 < idealSize);
    }

    @Test
    public void testAddParallel() throws Exception {
        AtomicLongArray locations = new AtomicLongArray(1000);
        CompactingBuffer buckets = new CompactingBuffer(20, new CompactionCallback(locations));
        ConcurrentHashMap<Integer, Integer> expected = new ConcurrentHashMap<>();
        LockManager lockManager = new LockManager();
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() / 2);

        for (int r = 0; r < 1000; r++) {
            for (int i = 0; i < locations.length(); i++) {
                executorService.submit(() -> {
                    try {
                        int key = ThreadLocalRandom.current().nextInt(locations.length());
                        int value = ThreadLocalRandom.current().nextInt();
                        Lock lock = lockManager.lock(key);
                        try {
                            locations.set(key, buckets.append(bufferForEntry(key, value)));
                            expected.put(key, value);
                        } finally {
                            lock.unlock();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            executorService.submit(() -> {
                try {
                    buckets.compact();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.DAYS);
        buckets.compact();
        buckets.compact();

        checkBuckets(expected, locations, buckets);

        checkMemoryUsage(expected, buckets);
    }

    private void checkBuckets(Map<Integer, Integer> expected, AtomicLongArray locations, CompactingBuffer buckets) {
        expected.entrySet().forEach(entry -> {
            buckets.access(() -> locations.get(entry.getKey()), buffer -> {
                assertEquals((int) entry.getKey(), keyFromBuffer(buffer));
                assertEquals((int) entry.getValue(), valueFromBuffer(buffer));
            });
        });
    }

    private static int keyFromBuffer(ByteBuffer input) {
        return input.getInt(input.position());
    }

    private static int valueFromBuffer(ByteBuffer input) {
        return input.getInt(input.position() + 4);
    }

    private ByteBuffer bufferForEntry(int key, int value) {
        return BufferUtils.locate(ByteBuffer.allocate(8).putInt(key).putInt(value), 0, 8);
    }

    private static class CompactionCallback implements org.daniilguit.ohhm.CompactionCallback {
        private final AtomicLongArray locations;

        public CompactionCallback(AtomicLongArray locations) {
            this.locations = locations;
        }

        @Override
        public boolean isGarbage(ByteBuffer data, long location) {
            return locations.get(keyFromBuffer(data)) != location;
        }

        @Override
        public void updated(ByteBuffer input, long oldLocation, long newLocation) {
            int key = keyFromBuffer(input);
            locations.compareAndSet(key, oldLocation, newLocation);
        }
    }
}