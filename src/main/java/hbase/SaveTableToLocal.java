package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.*;

/**
 * Created by WaterYe on 18-5-14.
 */
class SaveTableToLocal extends Runner {
    private Configuration conf;
    SaveTableToLocal(Configuration conf) throws IOException {
        this.conf = conf;
    }

    private Connection connection;
    private Table table;
    @Override
    protected void pre() throws IOException {
        connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TableName.valueOf(conf.get("Table")));
    }

    @Override
    protected void run() throws IOException {
        File file = new File(conf.get("savePath"));
        if(!file.exists()) {
            file.createNewFile();
        }
        PrintWriter pw = new PrintWriter(file);
        ResultScanner rs = table.getScanner(new Scan());
        StringBuilder sb = new StringBuilder();
        for(Result r : rs) {
            String word = new String(r.getRow());
            String value = new String(r.getValue("count".getBytes(), null));
            sb.append(word + "\t" + value + "\n");
        }
        pw.print(sb.toString());
        pw.flush();
        pw.close();
    }

    @Override
    protected void end() throws IOException {
        table.close();
        connection.close();
    }
}
