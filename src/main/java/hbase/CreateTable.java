package hbase;

import tools.Runner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

class CreateTable extends Runner {
  CreateTable(Configuration conf) throws IOException {
    super(conf);
  }

  private Connection connection;
  private Admin admin;
  @Override
  protected void pre() throws IOException {
    connection = ConnectionFactory.createConnection(conf);
    admin = connection.getAdmin();
  }

  @Override
  protected void run() throws IOException {
    TableName table = TableName.valueOf(conf.get("table"));

    if (admin.tableExists(table)) {
      admin.disableTable(table);
      admin.deleteTable(table);
    }

    // add column family
    HTableDescriptor tableDescriptor = new HTableDescriptor(table);
    HColumnDescriptor columnDescriptor = new HColumnDescriptor(conf.get("columnFamily"));
    tableDescriptor.addFamily(columnDescriptor);
    admin.createTable(tableDescriptor);
  }

  @Override
  protected void end() throws IOException {
    admin.close();
    connection.close();
  }
}