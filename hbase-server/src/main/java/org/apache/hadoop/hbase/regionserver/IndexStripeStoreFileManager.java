package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;

/**
 * Created by co2y on 22/11/2017.
 */
public class IndexStripeStoreFileManager extends StripeStoreFileManager{
    public IndexStripeStoreFileManager(KeyValue.KVComparator kvComparator, Configuration conf, StripeStoreConfig config) {
        super(kvComparator, conf, config);
    }
}
