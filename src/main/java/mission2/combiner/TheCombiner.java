package mission2.combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import mission2.tools.TheKey;

public class TheCombiner extends Reducer<TheKey, IntWritable, TheKey, IntWritable> {
  private final IntWritable intw = new IntWritable();

  @Override
  protected void reduce(TheKey key, Iterable<IntWritable> value,
      Reducer<TheKey, IntWritable, TheKey, IntWritable>.Context context) throws IOException, InterruptedException {
    int sum = 0;

    for (IntWritable i : value) {
      sum += i.get();
    }

    intw.set(sum);
    context.write(key, intw);
  }
}