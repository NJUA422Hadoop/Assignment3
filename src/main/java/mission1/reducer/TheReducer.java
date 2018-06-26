package mission1.reducer;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TheReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
  private final StringBuilder sb = new StringBuilder();

  @Override
  protected void reduce(NullWritable key, Iterable<Text> value, Reducer<NullWritable, Text, NullWritable, Text>.Context context)
    throws IOException, InterruptedException {
    sb.delete(0, sb.length());

    value.forEach(new Consumer<Text>() {
      @Override
      public void accept(Text t) {
        sb.append(t);
        sb.append(" ");
      }
    });
    
    context.write(NullWritable.get(), new Text(sb.toString()));
  }
}