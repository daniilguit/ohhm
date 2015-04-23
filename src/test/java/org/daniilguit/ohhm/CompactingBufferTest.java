package org.daniilguit.ohhm;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

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
            locations[i] = buckets.append(bufferForInt(i));
        }
        for (int i = 0; i < 1000; i++) {
            int ii = i;
            buckets.evaluate(locations[ii], buffer -> {
                if (!buffer.equals(bufferForInt(ii))) {
                    System.out.println("Diff on " + ii);
                    assertEquals(buffer, bufferForInt(ii));
                }
                return null;
            });
        }
        System.out.println(buckets.memoryUsage());
    }

    @Test
    public void testCompact() throws Exception {
        CompactingBuffer buckets = new CompactingBuffer(100);
        long[] locations = new long[1000];
        for (int i = 0; i < locations.length; i++) {
            locations[i] = buckets.append(bufferForInt(i));
        }
        CompactionCallback callback = (input, newLocation) -> {
            int key = keyFromBuffer(input);
            locations[key] = newLocation;
        };
        long memoryUsage = buckets.memoryUsage();
        Random r = new Random(0);
        for (int repeat = 0; repeat < 1000; repeat++) {
            for (int i = 0; i < locations.length; i += 3) {
                int index = r.nextInt(locations.length);
                locations[index] = buckets.append(bufferForInt(index), locations[index]);
            }
            buckets.compact(callback);
        }
        for (int i = 0; i < locations.length; i++) {
            int ii = i;
            buckets.evaluate(locations[ii], buffer -> {
                if (!buffer.equals(bufferForInt(ii))) {
                    System.out.println("Diff on " + ii);
                    System.out.println("Diff at " + (locations[ii] >> 32) + " -> " + (int) locations[ii]);
                    System.out.println("Diff " + keyFromBuffer(buffer) + " != " + ii);
                    assertEquals(ii, keyFromBuffer(buffer));
                }
                return null;
            });
        }
        assertTrue(memoryUsage * 3 > buckets.memoryUsage());
    }

    private static int keyFromBuffer(ByteBuffer input) {
        int position = input.position();
        byte[] data = new byte[input.remaining()];
        input.get(data);
        input.position(position);
        return Integer.valueOf(new String(data));
    }

    private ByteBuffer bufferForInt(int i) {
        return ByteBuffer.wrap(("" + i).getBytes());
    }
}