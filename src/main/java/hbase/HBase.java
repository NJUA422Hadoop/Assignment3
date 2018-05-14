package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * 之后对HBase的操作，就在这个类中实现，或者在同层的hbase包中实现，然后在这个类中提供接口。
 * @author RailgunHamster
 * @version 1.1
 * @date 2018/5/14
 */
public class HBase {
    static Configuration conf = HBaseConfiguration.create();
    static {
        conf.set("hbase.zookeeper.quorum", "localhost");
    }

    /**
     * create table
     * @author RailgunHamster
     */
    public void createTable(String table) {
        try {
            new CreateTable(conf, table).run();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }
    }

    /**
     * save from hbase to local
     * @author WaterYe
     */
    public void saveToLocal(String table) {
        try {
            new SaveTableToLocal(table).run();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }
    }
}