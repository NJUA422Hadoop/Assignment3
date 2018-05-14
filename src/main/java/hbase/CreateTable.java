package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

class CreateTable {
    private Configuration conf;
    private String table;
    CreateTable(Configuration conf, String table) {
        this.conf = conf;
        this.table = table;
    }

    void run() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
    }
}