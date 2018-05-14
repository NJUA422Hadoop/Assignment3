package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

class CreateTable {
    private Configuration conf;
    CreateTable(Configuration conf) {
        this.conf = conf;
    }

    void run() throws IOException {
        String table = this.conf.get("table");
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(table)) {
            throw new IOException("table already exists");
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(table);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(conf.get("columnFamily"));
        tableDescriptor.addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);
    }
}