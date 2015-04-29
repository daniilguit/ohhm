package org.daniilguit.ohhm;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Daniil Gitelson on 23.04.15.
 */
public class LockManager {

    private final Lock[] locks = new ReentrantLock[127];

    {
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    public Lock lock(int key) {
        Lock lock = locks[key % locks.length];
        lock.lock();
        return lock;
    }
}
