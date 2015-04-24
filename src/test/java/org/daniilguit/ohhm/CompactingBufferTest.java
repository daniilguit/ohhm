package org.daniilguit.ohhm;

import org.daniilguit.ohhm.util.BufferUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Daniil Gitelson on 23.04.15.
 */
public class CompactingBufferTest {
    @Test
    public void testAdd() throws Exception {
        CompactingBuffer buckets = new CompactingBuffer(100);
        long[] locations = new long[1000];
        for (int i = 0; i < locations.length; i++) {
            locations[i] = buckets.append(bufferForEntry(i, i));
        }
        for (int i = 0; i < locations.length; i++) {
            int ii = i;
            buckets.access(() -> locations[ii], buffer -> {
                assertEquals(buffer, bufferForEntry(ii, ii));
            });
        }
        System.out.println(buckets.memoryUsage());
    }

    @Test
    public void testCompact() throws Exception {
        long[] locations = new long[1000];
        CompactingBuffer buckets = new CompactingBuffer(100);
        Map<Integer, Integer> expected = new HashMap<>();
        for (int i = 0; i < locations.length; i++) {
            locations[i] = buckets.append(bufferForEntry(i, i));
            expected.put(i, i);
        }
        CompactionCallback callback = new CompactionCallback() {
            @Override
            public void updated(ByteBuffer input, long newLocation) {
                int key = keyFromBuffer(input);
                locations[key] = newLocation;
            }
        };
        Random r = new Random(0);
        for (int repeat = 0; repeat < 200; repeat++) {
            for (int i = 0; i < locations.length / 100; i += 3) {
                int index = r.nextInt(locations.length);
                locations[index] = buckets.append(bufferForEntry(index, index + repeat), locations[index]);
                expected.put(index, index + repeat);
            }
            for (int i = 0; i < locations.length; i++) {
                int ii = i;
                buckets.access(() -> locations[ii], buffer -> {
                    assertEquals(ii, keyFromBuffer(buffer));
                    assertEquals((long) expected.get(ii), (long) valueFromBuffer(buffer));
                });
            }
            buckets.compact(callback);
        }
        for (int i = 0; i < locations.length; i++) {
            int ii = i;
            buckets.access(() -> locations[ii], buffer -> {
                int key = keyFromBuffer(buffer);
                ByteBuffer expectedBuffer = bufferForEntry(key, expected.get(key));
                if (!buffer.equals(expectedBuffer)) {
                    System.out.println("Diff on " + ii);
                    System.out.println("Diff at " + locations[ii]);
                    System.out.println("Diff at " + (locations[ii] / 100) + " -> " + (locations[ii] % 100));
                    System.out.println("Diff " + key + " != " + ii);
                    System.out.println("Diff " + expected.get(key) + " != " + valueFromBuffer(buffer));
                    assertEquals(expectedBuffer, buffer);
                }
            });
        }
        System.out.println("Memory efficiency: " + buckets.memoryEfficiency());
        assertTrue(buckets.memoryEfficiency() > 0.5);
    }

    @Test
    public void testAddParallel() throws Exception {
        AtomicLongArray locations = new AtomicLongArray(1000);
        CompactingBuffer buckets = new CompactingBuffer(20);
        Map<Integer, Integer> expected = new ConcurrentHashMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        CompactionCallback callback = new CompactionCallback() {
            @Override
            public void updated(ByteBuffer input, long newLocation) {
                int key = keyFromBuffer(input);
                locations.set(key, newLocation);
            }
        };


        for (int r = 0; r < 100; r++) {
            for (int i = 0; i < locations.length(); i++) {
                int ii = i;
                executorService.submit(() -> {
                    try {
                        locations.set(ii, buckets.append(bufferForEntry(ii, ii)));
                        expected.put(ii, ii);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            Thread.sleep(1);
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        buckets.compact(callback);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.DAYS);

        for (int i = 0; i < locations.length(); i++) {
            int ii = i;
            long location = locations.get(ii);
            buckets.access(() -> location, buffer -> {
                int key = keyFromBuffer(buffer);
                ByteBuffer expectedBuffer = bufferForEntry(key, expected.get(key));
                if (!buffer.equals(expectedBuffer)) {
                    System.out.println("Diff on " + ii);
                    System.out.println("Diff at " + location);
                    System.out.println("Diff at " + (location / 100) + " -> " + (location % 100));
                    System.out.println("Diff " + key + " != " + ii);
                    System.out.println("Diff " + expected.get(key) + " != " + valueFromBuffer(buffer));
                    assertEquals(expectedBuffer, buffer);
                }
            });
        }
        System.out.println("Memory efficiency: " + buckets.memoryEfficiency());
        assertTrue(buckets.memoryEfficiency() > 0.5);
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
}