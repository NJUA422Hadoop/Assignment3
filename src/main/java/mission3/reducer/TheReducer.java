package mission3.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mission3.tools.TheValue;

public class TheReducer extends Reducer<Text, TheValue, Text, Text> {
  private final Text text = new Text();
  private final StringBuilder stringBuilder = new StringBuilder();

  @Override
  protected void reduce(Text key, Iterable<TheValue> value, Reducer<Text, TheValue, Text, Text>.Context context)
    throws IOException, InterruptedException {
    stringBuilder.delete(0, stringBuilder.length());

    int sum = 0;
    List<TheValue> list = new ArrayList<>();
    // value只能遍历一次，如果需要重复使用，请自行保存值
    for (TheValue v : value) {
      int count = v.second;
      // 这里不能直接保存v，因为Iterable在下一次get之后，会跟着改变。
      list.add(new TheValue(v.first, count));
      sum += count;
    }

    if (sum == 0) {
      return;
    }

    float fsum = sum;

    stringBuilder.append("[");
    for (TheValue v : list) {
      stringBuilder.append(v.first);
      stringBuilder.append(":");
      stringBuilder.append(v.second / fsum);
      stringBuilder.append("|");
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    stringBuilder.append("]");

    text.set(stringBuilder.toString());
    context.write(key, text);
  }
}