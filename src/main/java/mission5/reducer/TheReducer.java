package mission5.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TheReducer extends Reducer<Text, Text, Text, Text> {
  private static final Text text = new Text();

  @Override
  protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
    for(Text v : value) {
      text.set(v);
      context.write(key, text);
    }
  }
}
