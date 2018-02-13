package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by co2y on 22/11/2017.
 */
public class IndexKeyCompactionPolicy extends ExploringCompactionPolicy {

    private static final Log LOG = LogFactory.getLog(IndexKeyCompactionPolicy.class);

    /**
     * Constructor for ExploringCompactionPolicy.
     *
     * @param conf            The configuration object
     * @param storeConfigInfo An object to provide info about the store.
     */
    public IndexKeyCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
        super(conf, storeConfigInfo);
    }

    @Override
    public List<HStoreFile> applyCompactionPolicy(List<HStoreFile> candidates, boolean mightBeStuck, boolean isOffpeak, int minFiles, int maxFiles) {
        if (candidates.size() < minFiles) return new ArrayList<>(0);

        return new ArrayList<>(candidates);

//        final double currentRatio = isOffpeak
//                ? comConf.getCompactionRatioOffPeak() : comConf.getCompactionRatio();
//
//        // Start off choosing nothing.
//        List<StoreFile> bestSelection = new ArrayList<StoreFile>(0);
//        List<StoreFile> smallest = mightBeStuck ? new ArrayList<StoreFile>(0) : null;
//        long bestSize = 0;
//        long smallestSize = Long.MAX_VALUE;
//
//        int opts = 0, optsInRatio = 0, bestStart = -1; // for debug logging
//        // Consider every starting place.
//        for (int start = 0; start < candidates.size(); start++) {
//            // Consider every different sub list permutation in between start and end with min files.
//            for (int currentEnd = start + minFiles - 1;
//                 currentEnd < candidates.size(); currentEnd++) {
//                List<StoreFile> potentialMatchFiles = candidates.subList(start, currentEnd + 1);
//
//                // Sanity checks
//                if (potentialMatchFiles.size() < minFiles) {
//                    continue;
//                }
//                if (potentialMatchFiles.size() > maxFiles) {
//                    continue;
//                }
//
//                // Compute the total size of files that will
//                // have to be read if this set of files is compacted.
//                long size = getTotalStoreSize(potentialMatchFiles);
//
//                // Store the smallest set of files.  This stored set of files will be used
//                // if it looks like the algorithm is stuck.
//                if (mightBeStuck && size < smallestSize) {
//                    smallest = potentialMatchFiles;
//                    smallestSize = size;
//                }
//
//                if (size > comConf.getMaxCompactSize(isOffpeak)) {
//                    continue;
//                }
//
//                ++opts;
//                if (size >= comConf.getMinCompactSize()
//                        && !filesInRatio(potentialMatchFiles, currentRatio)) {
//                    continue;
//                }
//
//                ++optsInRatio;
//                if (isBetterSelection(bestSelection, bestSize, potentialMatchFiles, size, mightBeStuck)) {
//                    bestSelection = potentialMatchFiles;
//                    bestSize = size;
//                    bestStart = start;
//                }
//            }
//        }
//        if (bestSelection.size() == 0 && mightBeStuck) {
//            LOG.debug("IndexKeyCompactionPolicy compaction algorithm has selected " + smallest.size()
//                    + " files of size " + smallestSize + " because the store might be stuck");
//            return new ArrayList<StoreFile>(smallest);
//        }
//        LOG.debug("IndexKeyCompactionPolicy compaction algorithm has selected " + bestSelection.size()
//                + " files of size " + bestSize + " starting at candidate #" + bestStart +
//                " after considering " + opts + " permutations with " + optsInRatio + " in ratio");
//        return new ArrayList<StoreFile>(bestSelection);

//        // Start off choosing nothing.
//        List<StoreFile> bestSelection = new ArrayList<StoreFile>(0);
//        List<StoreFile> smallest = mightBeStuck ? new ArrayList<StoreFile>(0) : null;
//        long bestSize = 0;
//        long smallestSize = Long.MAX_VALUE;
//
//
//        for (int first = 0; first < candidates.size(); first++) {
//            StoreFile firstSF = candidates.get(first);
//            LinkedList<StoreFile> potentialSFs = new LinkedList<>();
//            potentialSFs.add(firstSF);
//            for (int second = first + 1; second < candidates.size(); second++) {
//                StoreFile secondSF = candidates.get(second);
//                if (isOverlap(firstSF, secondSF)) {
//                    potentialSFs.add(secondSF);
//
//                    long size = getTotalStoreSize(potentialSFs);
//
//                    if (isBetterSelection(bestSelection, bestSize, potentialSFs, size, false)) {
//                        bestSelection = potentialSFs;
//                        bestSize = size;
//                    }
//
//                    potentialSFs.removeLast();
//                }
//            }
//            potentialSFs.removeLast();
//        }
//
//        return new ArrayList<>(bestSelection);
    }


    /**
     * Find the total size of a list of store files.
     *
     * @param potentialMatchFiles StoreFile list.
     * @return Sum of StoreFile.getReader().length();
     */
    private long getTotalStoreSize(final List<HStoreFile> potentialMatchFiles) {
        long size = 0;

        for (HStoreFile s : potentialMatchFiles) {
            size += s.getReader().length();
        }
        return size;
    }

    private boolean isBetterSelection(List<StoreFile> bestSelection,
                                      long bestSize, List<StoreFile> selection, long size, boolean mightBeStuck) {

        // Keep if this gets rid of more files.  Or the same number of files for less io.
        return selection.size() > bestSelection.size()
                || (selection.size() == bestSelection.size() && size < bestSize);
    }

    private boolean filesInRatio(final List<HStoreFile> files, final double currentRatio) {
        if (files.size() < 2) {
            return true;
        }

        long totalFileSize = getTotalStoreSize(files);

        for (HStoreFile file : files) {
            long singleFileSize = file.getReader().length();
            long sumAllOtherFileSizes = totalFileSize - singleFileSize;

            if (singleFileSize > sumAllOtherFileSizes * currentRatio) {
                return false;
            }
        }
        return true;
    }
}
