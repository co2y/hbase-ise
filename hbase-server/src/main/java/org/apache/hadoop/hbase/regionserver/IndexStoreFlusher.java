package org.apache.hadoop.hbase.regionserver;

/**
 * Created by co2y on 16/10/2017.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Private
public class IndexStoreFlusher extends StoreFlusher {
    private static final Log LOG = LogFactory.getLog(IndexStoreFlusher.class);
    private final Object flushLock = new Object();

    public IndexStoreFlusher(Configuration conf, Store store) {
        super(conf, store);
    }

    @Override
    public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushId,
                                    MonitoredTask status) throws IOException {
        return flushIndexSnapshot(snapshot, cacheFlushId, status);
    }

    /**
     * New
     */
    @Override
    public List<Path> flushIndexSnapshot(MemStoreSnapshot snapshot, long cacheFlushId,
                                         MonitoredTask status) throws IOException {
        ArrayList<Path> result = new ArrayList<Path>();
        int cellsCount = snapshot.getCellsCount();
        if (cellsCount == 0) return result; // don't flush if there are no entries

        // Use a store scanner to find which rows to flush.
        long smallestReadPoint = store.getSmallestReadPoint();
        InternalScanner scanner = createIndexScanner(snapshot.getScanner(), smallestReadPoint);
        if (scanner == null) {
            return result; // NULL scanner returned from coprocessor hooks means skip normal processing
        }

        StoreFile.Writer writer;
        try {
            // TODO:  We can fail in the below block before we complete adding this flush to
            //        list of store files.  Add cleanup of anything put on filesystem if we fail.
            synchronized (flushLock) {
                status.setStatus("Flushing " + store + ": creating writer");
                // Write the map out to the disk
                writer = store.createWriterInTmp(cellsCount, store.getFamily().getCompression(),
            /* isCompaction = */ false,
            /* includeMVCCReadpoint = */ true,
            /* includesTags = */ snapshot.isTagsPresent(),
            /* shouldDropBehind = */ false);
                writer.setTimeRangeTracker(snapshot.getTimeRangeTracker());
                IOException e = null;
                try {
                    performFlush(scanner, writer, smallestReadPoint);
                } catch (IOException ioe) {
                    e = ioe;
                    // throw the exception out
                    throw ioe;
                } finally {
                    if (e != null) {
                        writer.close();
                    } else {
                        finalizeWriter(writer, cacheFlushId, status);
                    }
                }
            }
        } finally {
            scanner.close();
        }
        LOG.info("Flushed, sequenceid=" + cacheFlushId + ", memsize="
                + StringUtils.humanReadableInt(snapshot.getSize()) +
                ", hasBloomFilter=" + writer.hasGeneralBloom() +
                ", into tmp file " + writer.getPath());
        result.add(writer.getPath());
        return result;
    }
}

