package mission3.reducer;

import java.io.IOException;

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
    for (TheValue v : value) {
      sum += v.second;
    }

    if (sum == 0) {
      return;
    }

    float fsum = sum;

    stringBuilder.append("[");
    for (TheValue v : value) {
      stringBuilder.append(v.first);
      stringBuilder.append(":");
      stringBuilder.append(v.second / fsum);
      stringBuilder.append("|");
    }
    stringBuilder.append("]");

    context.write(key, new Text(stringBuilder.toString()));
  }
}