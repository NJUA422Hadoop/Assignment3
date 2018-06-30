package mission3.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mission3.tools.TheValue;
import tools.Tuple;

public class TheMapper extends Mapper<Text, Text, Text, TheValue> {
  /**
   * 将输入的key、value对，映射成Tuple<Text, TheValue>并返回。
   * <pre>
   * 例：
   * 输入: Tuple&lt;A,B&gt; C
   * 输出: A Tuple&lt;B,C&gt;
   * </pre>
   * @param tuple,count
   * @return name, tuple
   */
  public Tuple<Text, TheValue> split(Text tuple, Text count) {
    String _tuple = tuple.toString().trim();
    String[] names = _tuple.substring(1, _tuple.length() - 1).split(",");
    return new Tuple<>(
      new Text(names[0]),
      new TheValue(
        names[1],
        Integer.valueOf(count.toString().trim())
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
