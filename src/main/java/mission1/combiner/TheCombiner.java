package mission1.combiner;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mission1.tools.TheKey;

public class TheCombiner extends Reducer<TheKey, Text, TheKey, Text> {
  private final StringBuilder sb = new StringBuilder();

  @Override
  protected void reduce(TheKey key, Iterable<Text> value, Reducer<TheKey, Text, TheKey, Text>.Context context)
    throws IOException, InterruptedException {
    sb.delete(0, sb.length());

    value.forEach(new Consumer<Text>() {
      @Override
      public void accept(Text t) {
        sb.append(t);
        sb.append(" ");
      }
    });
    
    context.write(key, new Text(sb.toString().trim()));
  }
}