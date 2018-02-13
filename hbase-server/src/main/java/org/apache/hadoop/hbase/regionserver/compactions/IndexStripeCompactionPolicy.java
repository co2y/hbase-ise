package org.apache.hadoop.hbase.regionserver.compactions;

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StripeStoreConfig;
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
        ArrayList<ImmutableList<StoreFile>> stripes = si.getStripes();

        int bqIndex = -1;
        List<StoreFile> bqSelection = null;
        int stripeCount = stripes.size();
        long bqTotalSize = -1;
        for (int i = 0; i < stripeCount; ++i) {
            // If we want to compact L0 to drop deletes, we only want whole-stripe compactions.
            // So, pass includeL0 as 2nd parameter to indicate that.
            List<StoreFile> selection = selectSimpleCompaction(stripes.get(i),
                    !canDropDeletesWithoutL0 && includeL0, isOffpeak);
            if (selection.isEmpty()) continue;
            long size = 0;
            for (StoreFile sf : selection) {
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
        List<StoreFile> filesToCompact = new ArrayList<StoreFile>(bqSelection);
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
            List<StoreFile> l0Files = si.getLevel0Files();
            LOG.debug("Adding " + l0Files.size() + " files to compaction to be able to drop deletes");
            ConcatenatedLists<StoreFile> sfs = new ConcatenatedLists<StoreFile>();
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

    private List<StoreFile> selectSimpleCompaction(
            List<StoreFile> sfs, boolean allFilesOnly, boolean isOffpeak) {
        int minFilesLocal = Math.max(
                allFilesOnly ? sfs.size() : 0, this.config.getStripeCompactMinFiles());
        int maxFilesLocal = Math.max(this.config.getStripeCompactMaxFiles(), minFilesLocal);
        return stripePolicy.applyCompactionPolicy(sfs, false, isOffpeak, minFilesLocal, maxFilesLocal);
    }


}
