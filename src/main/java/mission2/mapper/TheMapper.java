package mission2.mapper;

import java.io.IOException;
import java.util.Arrays;
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
    // hadoop系统中保存的文件会有一些'\x00'无效字符，需要用trim消除，否则会产生bug。
    names.addAll(Arrays.asList(value.toString().trim().split(" ")));

    for (String a : names) {
      for (String b : names) {
        if (a.equals(b)) {
          continue;
        }

        context.write(new TheKey(a, b), one);
      }
    }

    names.clear();
  }
}