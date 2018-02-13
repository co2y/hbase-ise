package org.apache.hadoop.hbase.regionserver;

/**
 * Created by co2y on 16/10/2017.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.ReflectionUtils;

import java.io.IOException;
import java.util.List;


@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class IndexStoreEngine extends StoreEngine<
        DefaultStoreFlusher, RatioBasedCompactionPolicy, IndexCompactor, DefaultStoreFileManager> {

    private static final Log LOG = LogFactory.getLog(IndexStoreEngine.class);


    private static final Class<? extends DefaultStoreFlusher>
            INDEX_STORE_FLUSHER_CLASS = DefaultStoreFlusher.class;
    private static final Class<? extends IndexCompactor>
            INDEX_COMPACTOR_CLASS = IndexCompactor.class;
    private static final Class<? extends RatioBasedCompactionPolicy>
            INDEX_COMPACTION_POLICY_CLASS = ExploringCompactionPolicy.class;

    @Override
    public boolean needsCompaction(List<StoreFile> filesCompacting) {
        return compactionPolicy.needsCompaction(
                this.storeFileManager.getStorefiles(), filesCompacting);
    }

    @Override
    protected void createComponents(
            Configuration conf, Store store, KeyValue.KVComparator kvComparator) throws IOException {
        String className = INDEX_COMPACTOR_CLASS.getName();

        try {
            compactor = ReflectionUtils.instantiateWithCustomCtor(className,
                    new Class[]{Configuration.class, Store.class}, new Object[]{conf, store});
        } catch (Exception e) {
            throw new IOException("Unable to load configured compactor '" + className + "'", e);
        }
        className = INDEX_COMPACTION_POLICY_CLASS.getName();
        try {
            compactionPolicy = ReflectionUtils.instantiateWithCustomCtor(className,
                    new Class[]{Configuration.class, StoreConfigInformation.class},
                    new Object[]{conf, store});
        } catch (Exception e) {
            throw new IOException("Unable to load configured compaction policy '" + className + "'", e);
        }
        storeFileManager = new DefaultStoreFileManager(kvComparator, conf, compactionPolicy.getConf());
        className = INDEX_STORE_FLUSHER_CLASS.getName();
        try {
            storeFlusher = ReflectionUtils.instantiateWithCustomCtor(className,
                    new Class[]{Configuration.class, Store.class}, new Object[]{conf, store});
        } catch (Exception e) {
            throw new IOException("Unable to load configured store flusher '" + className + "'", e);
        }
    }


    @Override
    public CompactionContext createCompaction() {
        return new IndexCompactionContext();
    }

    private class IndexCompactionContext extends CompactionContext {
        @Override
        public boolean select(List<StoreFile> filesCompacting, boolean isUserCompaction,
                              boolean mayUseOffPeak, boolean forceMajor) throws IOException {
            request = compactionPolicy.selectCompaction(storeFileManager.getStorefiles(),
                    filesCompacting, isUserCompaction, mayUseOffPeak, forceMajor);
            return request != null;
        }

        @Override
        public List<Path> compact(CompactionThroughputController throughputController)
                throws IOException {
            return compact(throughputController, null);
        }

        @Override
        public List<Path> compact(CompactionThroughputController throughputController, User user)
                throws IOException {
            return compactor.compact(request, throughputController, user);
        }

        @Override
        public List<StoreFile> preSelect(List<StoreFile> filesCompacting) {
            return compactionPolicy.preSelectCompactionForCoprocessor(
                    storeFileManager.getStorefiles(), filesCompacting);
        }
    }

}

