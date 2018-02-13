package org.apache.hadoop.hbase.regionserver.compactions;

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StripeStoreConfig;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConcatenatedLists;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by co2y on 22/11/2017.
 */
public class IndexStripeCompactionPolicy extends StripeCompactionPolicy {
    private final static Log LOG = LogFactory.getLog(IndexStripeCompactionPolicy.class);
    private StripeStoreConfig config;
    private IndexKeyCompactionPolicy stripePolicy = null;

    public IndexStripeCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo, StripeStoreConfig config) {
        super(conf, storeConfigInfo, config);
        this.config = config;
        this.stripePolicy = new IndexKeyCompactionPolicy(conf, storeConfigInfo);
    }

    @Override
    protected StripeCompactionRequest selectSingleStripeCompaction(StripeInformationProvider si, boolean includeL0,
                                                                   boolean canDropDeletesWithoutL0, boolean isOffpeak)
            throws IOException {
        ArrayList<org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList<HStoreFile>> stripes = si.getStripes();

        int bqIndex = -1;
        List<HStoreFile> bqSelection = null;
        int stripeCount = stripes.size();
        long bqTotalSize = -1;
        for (int i = 0; i < stripeCount; ++i) {
            // If we want to compact L0 to drop deletes, we only want whole-stripe compactions.
            // So, pass includeL0 as 2nd parameter to indicate that.
            List<HStoreFile> selection = selectSimpleCompaction(stripes.get(i),
                    !canDropDeletesWithoutL0 && includeL0, isOffpeak);
            if (selection.isEmpty()) continue;
            long size = 0;
            for (HStoreFile sf : selection) {
                size += sf.getReader().length();
            }
            if (bqSelection == null || selection.size() > bqSelection.size() ||
                    (selection.size() == bqSelection.size() && size < bqTotalSize)) {
                bqSelection = selection;
                bqIndex = i;
                bqTotalSize = size;
            }
        }
        if (bqSelection == null) {
            LOG.debug("No good compaction is possible in any stripe");
            return null;
        }
        List<HStoreFile> filesToCompact = new ArrayList<HStoreFile>(bqSelection);
        // See if we can, and need to, split this stripe.
        int targetCount = 1;
        long targetKvs = Long.MAX_VALUE;
        boolean hasAllFiles = filesToCompact.size() == stripes.get(bqIndex).size();
        String splitString = "";
        if (hasAllFiles && bqTotalSize >= config.getSplitSize()) {
            if (includeL0) {
                // We want to avoid the scenario where we compact a stripe w/L0 and then split it.
                // So, if we might split, don't compact the stripe with L0.
                return null;
            }
            Pair<Long, Integer> kvsAndCount = estimateTargetKvs(filesToCompact, config.getSplitCount());
            targetKvs = kvsAndCount.getFirst();
            targetCount = kvsAndCount.getSecond();
            splitString = "; the stripe will be split into at most "
                    + targetCount + " stripes with " + targetKvs + " target KVs";
        }

        LOG.debug("Found compaction in a stripe with end key ["
                + Bytes.toString(si.getEndRow(bqIndex)) + "], with "
                + filesToCompact.size() + " files of total size " + bqTotalSize + splitString);

        // See if we can drop deletes.
        StripeCompactionRequest req;
        if (includeL0) {
            assert hasAllFiles;
            List<HStoreFile> l0Files = si.getLevel0Files();
            LOG.debug("Adding " + l0Files.size() + " files to compaction to be able to drop deletes");
            ConcatenatedLists<HStoreFile> sfs = new ConcatenatedLists<HStoreFile>();
            sfs.addSublist(filesToCompact);
            sfs.addSublist(l0Files);
            req = new BoundaryStripeCompactionRequest(sfs, si.getStripeBoundaries());
        } else {
            req = new SplitStripeCompactionRequest(
                    filesToCompact, si.getStartRow(bqIndex), si.getEndRow(bqIndex), targetCount, targetKvs);
        }
        if (hasAllFiles && (canDropDeletesWithoutL0 || includeL0)) {
            req.setMajorRange(si.getStartRow(bqIndex), si.getEndRow(bqIndex));
        }
        req.getRequest().setOffPeak(isOffpeak);
        return req;
    }

    private Pair<Long, Integer> estimateTargetKvs(Collection<HStoreFile> files, double splitCount) {
        // If the size is larger than what we target, we don't want to split into proportionally
        // larger parts and then have to split again very soon. So, we will increase the multiplier
        // by one until we get small enough parts. E.g. 5Gb stripe that should have been split into
        // 2 parts when it was 3Gb will be split into 3x1.67Gb parts, rather than 2x2.5Gb parts.
        long totalSize = getTotalFileSize(files);
        long targetPartSize = config.getSplitPartSize();
        assert targetPartSize > 0 && splitCount > 0;
        double ratio = totalSize / (splitCount * targetPartSize); // ratio of real to desired size
        while (ratio > 1.0) {
            // Ratio of real to desired size if we increase the multiplier.
            double newRatio = totalSize / ((splitCount + 1.0) * targetPartSize);
            if ((1.0 / newRatio) >= ratio) break; // New ratio is < 1.0, but further than the last one.
            ratio = newRatio;
            splitCount += 1.0;
        }
        long kvCount = (long) (getTotalKvCount(files) / splitCount);
        return new Pair<>(kvCount, (int) Math.ceil(splitCount));
    }

    private static long getTotalKvCount(final Collection<HStoreFile> candidates) {
        long totalSize = 0;
        for (HStoreFile storeFile : candidates) {
            totalSize += storeFile.getReader().getEntries();
        }
        return totalSize;
    }

    private List<HStoreFile> selectSimpleCompaction(
            List<HStoreFile> sfs, boolean allFilesOnly, boolean isOffpeak) {
        int minFilesLocal = Math.max(
                allFilesOnly ? sfs.size() : 0, this.config.getStripeCompactMinFiles());
        int maxFilesLocal = Math.max(this.config.getStripeCompactMaxFiles(), minFilesLocal);
        return stripePolicy.applyCompactionPolicy(sfs, false, isOffpeak, minFilesLocal, maxFilesLocal);
    }

    /**
     * Request for stripe compactor that will cause it to split the source files into several
     * separate files at the provided boundaries.
     */
    private static class BoundaryStripeCompactionRequest extends StripeCompactionRequest {
        private final List<byte[]> targetBoundaries;

        /**
         * @param request          Original request.
         * @param targetBoundaries New files should be written with these boundaries.
         */
        public BoundaryStripeCompactionRequest(CompactionRequestImpl request,
                                               List<byte[]> targetBoundaries) {
            super(request);
            this.targetBoundaries = targetBoundaries;
        }

        public BoundaryStripeCompactionRequest(Collection<HStoreFile> files,
                                               List<byte[]> targetBoundaries) {
            this(new CompactionRequestImpl(files), targetBoundaries);
        }

        @Override
        public List<Path> execute(StripeCompactor compactor,
                                  ThroughputController throughputController, User user) throws IOException {
            return compactor.compact(this.request, this.targetBoundaries, this.majorRangeFromRow,
                    this.majorRangeToRow, throughputController, user);
        }
    }

    /**
     * Request for stripe compactor that will cause it to split the source files into several
     * separate files into based on key-value count, as well as file count limit.
     * Most of the files will be roughly the same size. The last file may be smaller or larger
     * depending on the interplay of the amount of data and maximum number of files allowed.
     */
    private static class SplitStripeCompactionRequest extends StripeCompactionRequest {
        private final byte[] startRow, endRow;
        private final int targetCount;
        private final long targetKvs;

        /**
         * @param request     Original request.
         * @param startRow    Left boundary of the range to compact, inclusive.
         * @param endRow      Right boundary of the range to compact, exclusive.
         * @param targetCount The maximum number of stripe to compact into.
         * @param targetKvs   The KV count of each segment. If targetKvs*targetCount is less than
         *                    total number of kvs, all the overflow data goes into the last stripe.
         */
        public SplitStripeCompactionRequest(CompactionRequestImpl request,
                                            byte[] startRow, byte[] endRow, int targetCount, long targetKvs) {
            super(request);
            this.startRow = startRow;
            this.endRow = endRow;
            this.targetCount = targetCount;
            this.targetKvs = targetKvs;
        }

        public SplitStripeCompactionRequest(
                Collection<HStoreFile> files, byte[] startRow, byte[] endRow, long targetKvs) {
            this(files, startRow, endRow, Integer.MAX_VALUE, targetKvs);
        }

        public SplitStripeCompactionRequest(Collection<HStoreFile> files,
                                            byte[] startRow, byte[] endRow, int targetCount, long targetKvs) {
            this(new CompactionRequestImpl(files), startRow, endRow, targetCount, targetKvs);
        }

        @Override
        public List<Path> execute(StripeCompactor compactor,
                                  ThroughputController throughputController, User user) throws IOException {
            return compactor.compact(this.request, this.targetCount, this.targetKvs, this.startRow,
                    this.endRow, this.majorRangeFromRow, this.majorRangeToRow, throughputController, user);
        }

        /**
         * Set major range of the compaction to the entire compaction range.
         * See {@link #setMajorRange(byte[], byte[])}.
         */
        public void setMajorRangeFull() {
            setMajorRange(this.startRow, this.endRow);
        }
    }

}
