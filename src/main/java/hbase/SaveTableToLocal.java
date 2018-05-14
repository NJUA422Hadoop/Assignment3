package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.*;

/**
 * Created by WaterYe on 18-5-14.
 */
class SaveTableToLocal {
    private Configuration conf;
    SaveTableToLocal(Configuration conf) {
        this.conf = conf;
    }

    void run() throws IOException {
        File file = new File(conf.get("savePath"));
        if(!file.exists()) {
            file.createNewFile();
        }
        FileWriter fw = new FileWriter(file, true);
        PrintWriter pw = new PrintWriter(file);
        Configuration configuration = HBaseConfiguration.create();
        HTable table = new HTable(configuration, conf.get("table"));
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
        fw.close();
    }
}
