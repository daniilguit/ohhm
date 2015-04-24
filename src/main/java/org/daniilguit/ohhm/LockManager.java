package org.daniilguit.ohhm;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Daniil Gitelson on 23.04.15.
 */
public class LockManager {
    private final ConcurrentHashMap<Integer, Lock> locks = new ConcurrentHashMap<>();

    public void lock(int key) {
        Lock lock = locks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
    }

    public void unlock(int key) {
        Lock lock = locks.get(key);
        lock.unlock();
        locks.remove(key, lock);
    }
}
