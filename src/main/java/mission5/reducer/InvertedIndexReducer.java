package mission5.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * @author WangYidong
 */
public class InvertedIndexReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable> {
  public final static String intervals = "[@#]";
  public final static String columnFamily = "information";
  public final static String column = "avg time";

  private String term = new String();
  private String last = " ";
  private int if_first=1;
  private int countItem;
  private int countDoc;
  private String out = new String();
  private Float f;
  private MultipleOutputs mos;
  private Configuration conf;

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    term = key.toString().split(intervals)[0];
    if (!term.equals(last)) {
      if (if_first==0){
        out=out.substring(0, out.length()-1);
        f = (float) countItem / countDoc;
        Put put = new Put(term.getBytes());
        put.addColumn(columnFamily.getBytes(),column.getBytes(),String.format("%.2f", f).getBytes());
        //HBASE
        context.write(new ImmutableBytesWritable(term.getBytes()), put);
        //HDFS
        mos.write("hdfs", new Text(term), out, this.conf.get("output"));
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

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    out=out.substring(0, out.length()-1);
    f = (float) countItem / countDoc;
    Put put = new Put(last.getBytes());
    put.addColumn(columnFamily.getBytes(),column.getBytes(),String.format("%.2f", f).getBytes());
    //HBASE
    context.write(new ImmutableBytesWritable(last.getBytes()),put);
    //HDFS
    mos.write("hdfs", new Text(last), out, this.conf.get("output"));
    //close
    mos.close();
  }

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    this.mos = new MultipleOutputs(context);
    this.conf = context.getConfiguration();
  }
}
