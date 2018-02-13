package org.apache.hadoop.hbase.regionserver;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.*;
import org.apache.hadoop.hbase.security.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by co2y on 22/11/2017.
 */

/**
 * The storage engine that implements the stripe-based store/compaction scheme.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class IndexStripeStoreEngine extends StoreEngine<IndexStripeStoreFlusher,
        IndexStripeCompactionPolicy, IndexStripeCompactor, IndexStripeStoreFileManager> {
    private static final Log LOG = LogFactory.getLog(IndexStripeStoreEngine.class);
    private StripeStoreConfig config;

    @Override
    public boolean needsCompaction(List<StoreFile> filesCompacting) {
        return this.compactionPolicy.needsCompactions(this.storeFileManager, filesCompacting);
    }

    @Override
    public CompactionContext createCompaction() {
        return new IndexStripeCompaction();
    }

    @Override
    protected void createComponents(
            Configuration conf, Store store, KeyValue.KVComparator comparator) throws IOException {
        this.config = new StripeStoreConfig(conf, store);
        this.compactionPolicy = new IndexStripeCompactionPolicy(conf, store, config);
        this.storeFileManager = new IndexStripeStoreFileManager(comparator, conf, this.config);
        this.storeFlusher = new IndexStripeStoreFlusher(
                conf, store, this.compactionPolicy, this.storeFileManager);
        this.compactor = new IndexStripeCompactor(conf, store);
    }

    /**
     * Represents one instance of stripe compaction, with the necessary context and flow.
     */
    private class IndexStripeCompaction extends CompactionContext {
        private IndexStripeCompactionPolicy.StripeCompactionRequest stripeRequest = null;

        @Override
        public List<StoreFile> preSelect(List<StoreFile> filesCompacting) {
            return compactionPolicy.preSelectFilesForCoprocessor(storeFileManager, filesCompacting);
        }

        @Override
        public boolean select(List<StoreFile> filesCompacting, boolean isUserCompaction,
                              boolean mayUseOffPeak, boolean forceMajor) throws IOException {
            this.stripeRequest = compactionPolicy.selectCompaction(
                    storeFileManager, filesCompacting, mayUseOffPeak);
            this.request = (this.stripeRequest == null)
                    ? new CompactionRequest(new ArrayList<StoreFile>()) : this.stripeRequest.getRequest();
            return this.stripeRequest != null;
        }

        @Override
        public void forceSelect(CompactionRequest request) {
            super.forceSelect(request);
            if (this.stripeRequest != null) {
                this.stripeRequest.setRequest(this.request);
            } else {
                LOG.warn("Stripe store is forced to take an arbitrary file list and compact it.");
                this.stripeRequest = compactionPolicy.createEmptyRequest(storeFileManager, this.request);
            }
        }

        @Override
        public List<Path> compact(CompactionThroughputController throughputController)
                throws IOException {
            Preconditions.checkArgument(this.stripeRequest != null, "Cannot compact without selection");
            return this.stripeRequest.execute(compactor, throughputController, null);
        }

        @Override
        public List<Path> compact(CompactionThroughputController throughputController, User user)
                throws IOException {
            Preconditions.checkArgument(this.stripeRequest != null, "Cannot compact without selection");
            return this.stripeRequest.execute(compactor, throughputController, user);
        }
    }
}

