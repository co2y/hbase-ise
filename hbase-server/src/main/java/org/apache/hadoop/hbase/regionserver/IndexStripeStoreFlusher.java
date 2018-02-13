package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.compactions.StripeCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;

import java.io.IOException;
import java.util.List;

/**
 * Created by co2y on 22/11/2017.
 */
public class IndexStripeStoreFlusher extends StripeStoreFlusher {
    private static final Log LOG = LogFactory.getLog(IndexStripeStoreFlusher.class);

    public IndexStripeStoreFlusher(Configuration conf, HStore store, StripeCompactionPolicy policy, StripeStoreFileManager stripes) {
        super(conf, store, policy, stripes);
    }

    @Override
    public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushSeqNum,
                                    MonitoredTask status, ThroughputController throughputController,
                                    FlushLifeCycleTracker tracker) throws IOException {

        LOG.error("index flush");
        return flushIndexSnapshot(snapshot, cacheFlushSeqNum, status, throughputController, tracker);
    }

}
