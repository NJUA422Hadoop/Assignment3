package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

class CreateTable {
    private Configuration conf;
    private String table;
    private String columnFamily;
    CreateTable(Configuration conf, String table) {
        this.conf = conf;
        this.table = table;
    }

    void run() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(this.table)) {
            throw new IOException("table already exists");
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(this.table);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(this.columnFamily);
        tableDescriptor.addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);
    }
}