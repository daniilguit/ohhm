package org.daniilguit.ohhm;

/**
 * Created by Daniil Gitelson on 23.04.15.
 */
public class OffHeapHashMapConfig {
    private static final int MEGABYTE = 1 << 20;

    public int initialSize = 1023;
    public int bucketSize = MEGABYTE;
    public int partitions = Runtime.getRuntime().availableProcessors();
    public int compactionInterval = 60;
}
