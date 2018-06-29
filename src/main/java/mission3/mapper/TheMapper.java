package mission3.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mission3.tools.TheValue;
import tools.Tuple;

public class TheMapper extends Mapper<Object, Text, Text, TheValue> {
  private Tuple<Text, TheValue> split(Text line) {
    String[] info = line.toString().split(" ");
    String[] names = info[0].substring(1, info[0].length() - 1).split(",");
    return new Tuple<>(
      new Text(names[0]),
      new TheValue(
        names[1],
        Integer.valueOf(info[1])
      )
    );
  }

  @Override
  protected void map(Object key, Text value, Mapper<Object, Text, Text, TheValue>.Context context)
    throws IOException, InterruptedException {
    Tuple<Text, TheValue> info = split(value);
    context.write(info.first, info.second);
  }
}