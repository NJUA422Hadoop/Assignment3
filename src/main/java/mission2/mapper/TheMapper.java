package mission2.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mission2.tools.TheKey;

public class TheMapper extends Mapper<Object, Text, TheKey, IntWritable> {
  private final static IntWritable one = new IntWritable(1);

  @Override
  protected void map(Object key, Text value, Mapper<Object, Text, TheKey, IntWritable>.Context context)
    throws IOException, InterruptedException {
    String[] names = value.toString().split(" ");

    for (String a : names) {
      for (String b : names) {
        if (a == b) {
          continue;
        }

        context.write(
          new TheKey(
            new Text(a),
            new Text(b)
          ), one
        );
      }
    }
  }
}