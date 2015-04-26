package org.daniilguit.ohhm;

import org.daniilguit.ohhm.util.BufferUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Created by Daniil Gitelson on 23.04.15.
 */
@Ignore
public class OffHeapHashMapTest {

    @Test
    public void testSimple() throws Exception {
        Map<Integer, Integer> expected = new HashMap<>();
        Random random = new Random(0);
        OffHeapHashMap hashMap = new OffHeapHashMap(new OffHeapHashMapConfig());

        int N = 10000;

        for (int i = 0; i < 5 * N; i++) {
            int key = random.nextInt(N);
            int value = random.nextInt(N);

            expected.put(key, value);
            hashMap.entry(bufferForInt(key)).put(bufferForInt(value));
        }

        expected.entrySet().stream().forEach(entry -> {
            assertEquals((long)entry.getValue(), intFromBuffer(hashMap.entry(bufferForInt(entry.getKey())).get()));
        });
    }

    private int intFromBuffer(ByteBuffer buffer) {
        return buffer.getInt();
    }

    private ByteBuffer bufferForInt(int key) {
        return BufferUtils.locate(ByteBuffer.allocate(4).putInt(key), 0, 4);
    }

}