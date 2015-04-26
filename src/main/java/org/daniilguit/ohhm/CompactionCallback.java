package org.daniilguit.ohhm;

import java.nio.ByteBuffer;

/**
 * Created by Daniil Gitelson on 22.04.15.
 */
public interface CompactionCallback {
    void updated(ByteBuffer data, long oldLocation, long newLocation);
}
