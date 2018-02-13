package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
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

    public IndexStripeCompactor(Configuration conf, HStore store) {
        super(conf, store);
    }

    private final class StripeInternalScannerFactory implements InternalScannerFactory {

        private final byte[] majorRangeFromRow;

        private final byte[] majorRangeToRow;

        public StripeInternalScannerFactory(byte[] majorRangeFromRow, byte[] majorRangeToRow) {
            this.majorRangeFromRow = majorRangeFromRow;
            this.majorRangeToRow = majorRangeToRow;
        }

        @Override
        public ScanType getScanType(CompactionRequestImpl request) {
            // If majorRangeFromRow and majorRangeToRow are not null, then we will not use the return
            // value to create InternalScanner. See the createScanner method below. The return value is
            // also used when calling coprocessor hooks.
            return ScanType.INDEX_DELETES;
        }

        @Override
        public InternalScanner createScanner(ScanInfo scanInfo, List<StoreFileScanner> scanners,
                                             ScanType scanType, FileDetails fd, long smallestReadPoint) throws IOException {
            return (majorRangeFromRow == null)
                    ? IndexStripeCompactor.this.createScanner(store, scanInfo, scanners, scanType,
                    smallestReadPoint, fd.earliestPutTs)
                    : IndexStripeCompactor.this.createScanner(store, scanInfo, scanners, true, smallestReadPoint,
                    fd.earliestPutTs, majorRangeFromRow, majorRangeToRow);
        }
    }

    public List<Path> compact(CompactionRequestImpl request, final List<byte[]> targetBoundaries,
                              final byte[] majorRangeFromRow, final byte[] majorRangeToRow,
                              ThroughputController throughputController, User user) throws IOException {
        if (LOG.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Executing compaction with " + targetBoundaries.size() + " boundaries:");
            for (byte[] tb : targetBoundaries) {
                sb.append(" [").append(Bytes.toString(tb)).append("]");
            }
            LOG.debug(sb.toString());
        }
        return compact(request, new IndexStripeCompactor.StripeInternalScannerFactory(majorRangeFromRow, majorRangeToRow),
                new CellSinkFactory<StripeMultiFileWriter>() {

                    @Override
                    public StripeMultiFileWriter createWriter(InternalScanner scanner, FileDetails fd,
                                                              boolean shouldDropBehind) throws IOException {
                        StripeMultiFileWriter writer = new StripeMultiFileWriter.BoundaryMultiWriter(
                                store.getComparator(), targetBoundaries, majorRangeFromRow, majorRangeToRow);
                        initMultiWriter(writer, scanner, fd, shouldDropBehind);
                        return writer;
                    }
                }, throughputController, user);
    }

    public List<Path> compact(CompactionRequestImpl request, final int targetCount, final long targetSize,
                              final byte[] left, final byte[] right, byte[] majorRangeFromRow, byte[] majorRangeToRow,
                              ThroughputController throughputController, User user) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Executing compaction with " + targetSize + " target file size, no more than " + targetCount
                            + " files, in [" + Bytes.toString(left) + "] [" + Bytes.toString(right) + "] range");
        }
        return compact(request, new IndexStripeCompactor.StripeInternalScannerFactory(majorRangeFromRow, majorRangeToRow),
                new CellSinkFactory<StripeMultiFileWriter>() {

                    @Override
                    public StripeMultiFileWriter createWriter(InternalScanner scanner, FileDetails fd,
                                                              boolean shouldDropBehind) throws IOException {
                        StripeMultiFileWriter writer = new StripeMultiFileWriter.SizeMultiWriter(
                                store.getComparator(), targetCount, targetSize, left, right);
                        initMultiWriter(writer, scanner, fd, shouldDropBehind);
                        return writer;
                    }
                }, throughputController, user);
    }

    @Override
    protected List<Path> commitWriter(StripeMultiFileWriter writer, FileDetails fd,
                                      CompactionRequestImpl request) throws IOException {
        List<Path> newFiles = writer.commitWriters(fd.maxSeqId, request.isMajor());
        assert !newFiles.isEmpty() : "Should have produced an empty file to preserve metadata.";
        return newFiles;
    }

}
