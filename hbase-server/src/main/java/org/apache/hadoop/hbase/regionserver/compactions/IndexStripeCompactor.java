package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by co2y on 22/11/2017.
 */
public class IndexStripeCompactor extends StripeCompactor {
    private static final Log LOG = LogFactory.getLog(IndexStripeCompactor.class);

    public IndexStripeCompactor(Configuration conf, Store store) {
        super(conf, store);
    }

    @Override
    public List<Path> compact(CompactionRequest request, List<byte[]> targetBoundaries,
                              byte[] majorRangeFromRow, byte[] majorRangeToRow,
                              CompactionThroughputController throughputController) throws IOException {
        return compact(request, targetBoundaries, majorRangeFromRow, majorRangeToRow,
                throughputController, null);
    }

    @Override
    public List<Path> compact(CompactionRequest request, List<byte[]> targetBoundaries,
                              byte[] majorRangeFromRow, byte[] majorRangeToRow,
                              CompactionThroughputController throughputController, User user) throws IOException {
        if (LOG.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Executing compaction with " + targetBoundaries.size() + " boundaries:");
            for (byte[] tb : targetBoundaries) {
                sb.append(" [").append(Bytes.toString(tb)).append("]");
            }
            LOG.debug(sb.toString());
        }
        StripeMultiFileWriter writer = new StripeMultiFileWriter.BoundaryMultiWriter(
                targetBoundaries, majorRangeFromRow, majorRangeToRow);
        return compactInternal(writer, request, majorRangeFromRow, majorRangeToRow,
                throughputController, user);
    }

    @Override
    public List<Path> compact(CompactionRequest request, int targetCount, long targetSize,
                              byte[] left, byte[] right, byte[] majorRangeFromRow, byte[] majorRangeToRow,
                              CompactionThroughputController throughputController) throws IOException {
        return compact(request, targetCount, targetSize, left, right, majorRangeFromRow,
                majorRangeToRow, throughputController, null);
    }

    @Override
    public List<Path> compact(CompactionRequest request, int targetCount, long targetSize,
                              byte[] left, byte[] right, byte[] majorRangeFromRow, byte[] majorRangeToRow,
                              CompactionThroughputController throughputController, User user) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing compaction with " + targetSize
                    + " target file size, no more than " + targetCount + " files, in ["
                    + Bytes.toString(left) + "] [" + Bytes.toString(right) + "] range");
        }
        StripeMultiFileWriter writer = new StripeMultiFileWriter.SizeMultiWriter(
                targetCount, targetSize, left, right);
        return compactInternal(writer, request, majorRangeFromRow, majorRangeToRow,
                throughputController, user);
    }

    private List<Path> compactInternal(StripeMultiFileWriter mw, final CompactionRequest request,
                                       byte[] majorRangeFromRow, byte[] majorRangeToRow,
                                       CompactionThroughputController throughputController, User user) throws IOException {
        final Collection<StoreFile> filesToCompact = request.getFiles();
        final FileDetails fd = getFileDetails(filesToCompact, request.isMajor());
        this.progress = new CompactionProgress(fd.maxKeyCount);

        long smallestReadPoint = getSmallestReadPoint();
        List<StoreFileScanner> scanners = createFileScanners(filesToCompact,
                smallestReadPoint, store.throttleCompaction(request.getSize()));

        boolean finished = false;
        InternalScanner scanner = null;
        boolean cleanSeqId = false;
        try {
            // Get scanner to use.
            ScanType coprocScanType = ScanType.COMPACT_RETAIN_DELETES;
            scanner = preCreateCoprocScanner(request, coprocScanType, fd.earliestPutTs, scanners, user);
            if (scanner == null) {
                scanner = (majorRangeFromRow == null)
                        ? createScanner(store, scanners,
                        ScanType.INDEX_DELETES, smallestReadPoint, fd.earliestPutTs)
                        : createScanner(store, scanners, true,
                        smallestReadPoint, fd.earliestPutTs, majorRangeFromRow, majorRangeToRow);
            }
            scanner = postCreateCoprocScanner(request, coprocScanType, scanner, user);
            if (scanner == null) {
                // NULL scanner returned from coprocessor hooks means skip normal processing.
                return new ArrayList<Path>();
            }

            // Create the writer factory for compactions.
            if (fd.minSeqIdToKeep > 0) {
                smallestReadPoint = Math.min(fd.minSeqIdToKeep, smallestReadPoint);
                cleanSeqId = true;
            }

            final boolean needMvcc = fd.maxMVCCReadpoint > 0;

            final Compression.Algorithm compression = store.getFamily().getCompactionCompression();
            StripeMultiFileWriter.WriterFactory factory = new StripeMultiFileWriter.WriterFactory() {
                @Override
                public StoreFile.Writer createWriter() throws IOException {
                    return store.createWriterInTmp(
                            fd.maxKeyCount, compression, true, needMvcc, fd.maxTagsLength > 0,
                            store.throttleCompaction(request.getSize()));
                }
            };

            // Prepare multi-writer, and perform the compaction using scanner and writer.
            // It is ok here if storeScanner is null.
            StoreScanner storeScanner = (scanner instanceof StoreScanner) ? (StoreScanner) scanner : null;
            mw.init(storeScanner, factory, store.getComparator());
            finished =
                    performCompaction(scanner, mw, smallestReadPoint, cleanSeqId, throughputController);
            if (!finished) {
                throw new InterruptedIOException("Aborting compaction of store " + store +
                        " in region " + store.getRegionInfo().getRegionNameAsString() +
                        " because it was interrupted.");
            }
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (Throwable t) {
                    // Don't fail the compaction if this fails.
                    LOG.error("Failed to close scanner after compaction.", t);
                }
            }
            if (!finished) {
                for (Path leftoverFile : mw.abortWriters()) {
                    try {
                        store.getFileSystem().delete(leftoverFile, false);
                    } catch (Exception ex) {
                        LOG.error("Failed to delete the leftover file after an unfinished compaction.", ex);
                    }
                }
            }
        }

        assert finished : "We should have exited the method on all error paths";
        List<Path> newFiles = mw.commitWriters(fd.maxSeqId, request.isMajor());
        assert !newFiles.isEmpty() : "Should have produced an empty file to preserve metadata.";
        return newFiles;
    }

}
