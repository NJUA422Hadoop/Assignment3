package mission4.reducer;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TheReducer extends Reducer<Text, Text, Text, Text> {
  private static final double damping = 0.85;

  @Override
  protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
      String link = "";
      double pr = 0;
      for(Text v:values) {
          String tmp = v.toString();
          if(tmp.startsWith("|")) {
              link = tmp.substring(tmp.indexOf("|")+1);
          }
          else {
              pr += Double.parseDouble(v.toString());
          }
      }
      pr = 1 - damping + damping * pr;
      context.write(key, new Text(String.valueOf(pr) + "\t" + link));
  }
}