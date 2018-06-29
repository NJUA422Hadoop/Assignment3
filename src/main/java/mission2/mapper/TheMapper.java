package mission2.mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mission2.tools.TheKey;

public class TheMapper extends Mapper<Object, Text, TheKey, IntWritable> {
  private final Set<String> names = new HashSet<>();
  private final IntWritable one = new IntWritable(1);

  @Override
  protected void map(Object key, Text value, Mapper<Object, Text, TheKey, IntWritable>.Context context)
    throws IOException, InterruptedException {
    names.clear();

    for (String name : value.toString().split(" ")) {
      names.add(name);
    }

    for (String a : names) {
      for (String b : names) {
        if (a == b) {
          continue;
        }

        context.write(new TheKey(a, b), one);
      }
    }
  }
}