package org.apache.hadoop.hbase.regionserver.index;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.hadoop.hbase.util.Bytes;
import kafka.serializer.StringEncoder;
import kafka.javaapi.producer.Producer;

import java.util.Properties;

/**
 * Created by co2y on 30/10/2017.
 */
public class IndexUtil {
    private static Producer<String, String> producer;

    public static final byte[] INDEX_CF = Bytes.toBytes("family");
    public static final byte[] INDEX_COL = Bytes.toBytes("field0");

    static {
        Properties properties = new Properties();
//        properties.put("zookeeper.connect", "172.17.0.3:2181");
//        properties.put("metadata.broker.list", "172.17.0.3:9092");
        properties.put("zookeeper.connect", "slave206:2181,slave207:2181,slave208:2181,slave209:2181,slave210:2181/kafka");
        properties.put("metadata.broker.list", "slave206:9092,slave207:9092");
        properties.put("serializer.class", StringEncoder.class.getName());
        producer = new Producer<>(new ProducerConfig(properties));
    }

    static final String INDEX_ROWKEY_DELIMITER = "/";

    static final public byte[] INDEXTABLE_COLUMNFAMILY = Bytes.toBytes("cf"); //be consistent with column_family_name in weblog_cf_country (in current preloaded dataset)
    static final public byte[] INDEXTABLE_SPACEHOLDER = Bytes.toBytes("EMPTY");

    public static String[] parseIndexRowkey(byte[] indexRowkey) {
        return Bytes.toString(indexRowkey).split(INDEX_ROWKEY_DELIMITER);
    }

    public static byte[] generateIndexRowkey(byte[] dataKey, byte[] dataValue) {
        return Bytes.toBytes(Bytes.toString(dataValue) + INDEX_ROWKEY_DELIMITER + Bytes.toString(dataKey));
    }


    public static void sendToAUQ(String line, String table) {
        producer.send(new KeyedMessage<String, String>(table, line));
    }
}
