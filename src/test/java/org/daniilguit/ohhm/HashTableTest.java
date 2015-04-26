package org.daniilguit.ohhm;

import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

/**
 * Created by Daniil Gitelson on 24.04.15.
 */
public class HashTableTest {
    @Test
    public void testInsert() throws Exception {
        Map<Integer, Long> map = new ConcurrentHashMap<>();
        HashTable table = new HashTable(31);
        Random random = new Random(0);
        int N = 10000;
        for (int i = 0; i < N; i++) {
            int _key;
            do {
                _key = random.nextInt();
            } while (map.containsKey(_key));
            int key = _key;
            map.put(_key, (long) i);
            while (table.density() > 0.7 || table.upsert(_key, (location) -> location == map.getOrDefault(key, 0L), i) < 0) {
                HashTable newTable;
                int newSize = table.size();
                do {
                    newSize = newSize * 3 / 2;
                    newTable = new HashTable(newSize);
                } while (!newTable.insert(table));
                table = newTable;
            }
        }
        checkTable(map, table);
    }

    @Test
    public void testUpsert() throws Exception {
        Map<Integer, Long> map = new ConcurrentHashMap<>();
        HashTable table = new HashTable(31);
        Random random = new Random(0);
        int N = 10000;
        for (int i = 0; i < N; i++) {
            int key = random.nextInt(N / 10);
            map.put(key, (long) i);
            while (table.density() > 0.7 || table.upsert(key, (location) -> location == map.getOrDefault(key, 0L), i) < 0) {
                HashTable newTable;
                int newSize = table.size();
                do {
                    newSize = newSize * 3 / 2;
                    newTable = new HashTable(newSize);
                } while (!newTable.insert(table));
                table = newTable;
            }
        }
        checkTable(map, table);
    }

    private void checkTable(Map<Integer, Long> expected, HashTable table) {
        expected.entrySet().forEach(entry -> {
            assertEquals((long) entry.getValue(), table.get(entry.getKey(), (location) -> location == entry.getValue()));
        });
    }
}