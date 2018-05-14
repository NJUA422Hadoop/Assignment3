package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

// public class InvertedIndexReducer extends TableReducer<XXX>
public class InvertedIndexReducer extends TableReducer<Text,IntWritable,Text> {
  public final static Class<? extends Writable> outputKeyClass  =Text.class;
  public final static Class<? extends Writable> outputValueClass = Text.class;
  private String term = new String();
  private String last = " ";
  private int if_first=1;
  private int countItem;
  private int countDoc;
  private String out = new String();
  private float f;
  public static final String intervals = "[@#]";
  public final static String columnFamily= "information";
  public final static String column= "avg time";
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      term = key.toString().split(intervals)[0];
      if (!term.equals(last)) {
        if (if_first==0){
              out=out.substring(0, out.length()-1);
              f = (float) countItem / countDoc;
              Put put = new Put(key.getBytes());
              put.add(columnFamily.getBytes(),column.getBytes(),f.getBytes());
              context.write(new Text(last), new Text(String.format("%.2f,%s", f, out)));
              countItem = 0;
              countDoc = 0;
              out = new String();
          }
          last = term;
          if_first=0;
      }
      int sum = 0;
      for (IntWritable val : values) {
          sum += val.get();
      }
      out=out+key.toString().split(intervals)[1] + ":" + sum + ";";
      countItem += sum;
      countDoc += 1;
  }
  public void cleanup(Context context) throws IOException, InterruptedException {
      out=out.substring(0, out.length()-1);
      f = (float) countItem / countDoc;
      Put put = new Put(key.getBytes());
      put.add(columnFamily.getBytes(),column.getBytes(),f.getBytes());
      context.write(new Text(last), new Text(String.format("%.2f,%s", f, out)));
  }
}
