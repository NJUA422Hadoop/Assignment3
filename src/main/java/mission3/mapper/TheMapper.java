package mission3.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mission3.tools.TheValue;
import tools.Tuple;

public class TheMapper extends Mapper<Text, Text, Text, TheValue> {
  private Tuple<Text, TheValue> split(Text tuple, Text count) {
    String _tuple = tuple.toString();
    String[] names = _tuple.substring(1, _tuple.length() - 1).split(",");
    return new Tuple<>(
      new Text(names[0]),
      new TheValue(
        names[1],
        Integer.valueOf(count.toString())
      )
    );
  }

  @Override
  protected void map(Text key, Text value, Mapper<Text, Text, Text, TheValue>.Context context)
    throws IOException, InterruptedException {
    Tuple<Text, TheValue> info = split(key, value);
    context.write(info.first, info.second);
  }
}