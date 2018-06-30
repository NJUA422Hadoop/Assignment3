package mission3.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mission3.tools.TheValue;

public class TheReducer extends Reducer<Text, TheValue, Text, Text> {
  private final StringBuilder stringBuilder = new StringBuilder();

  @Override
  protected void reduce(Text key, Iterable<TheValue> value, Reducer<Text, TheValue, Text, Text>.Context context)
    throws IOException, InterruptedException {
    stringBuilder.delete(0, stringBuilder.length());

    int sum = 0;
    List<TheValue> list = new ArrayList<>();
    for (TheValue v : value) {
      list.add(v);
      sum += v.second;
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

    context.write(key, new Text(stringBuilder.toString()));
  }
}