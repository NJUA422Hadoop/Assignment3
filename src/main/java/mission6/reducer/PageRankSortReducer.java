package mission6.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;

public class PageRankSortReducer extends Reducer<FloatWritable, Text, FloatWritable, Text> {
    @Override
    protected void reduce(FloatWritable key, Iterable<Text> value, Reducer<FloatWritable, Text, FloatWritable, Text>.Context context)
        throws IOException, InterruptedException {
            for(Text v : value) {
                context.write(key, v);
        }
    }
}