package mission4.reducer;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TheReducer extends Reducer<Text, Text, Text, Text> {
  private final StringBuilder sb = new StringBuilder();

  @Override
  protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
    sb.delete(0, sb.length());

    for (Text t : value) {
      sb.append(t);
    }
    
    context.write(key, new Text(sb.toString()));
  }
}