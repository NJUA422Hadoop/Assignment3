package mission6.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;

public class PageRankSortReducer extends Reducer<FloatWritable, Text, FloatWritable, Text> {
    @Override
<<<<<<< HEAD
    protected void reduce(FloatWritable key, Iterable<Text> value, Reducer<FloatWritable, Text, FloatWritable, Text>.Context context)
=======
    protected void reduce(FloatWritable key, Iterable<Text> value, Reducer<FloatWritable, Text, Text, Text>.Context context)
>>>>>>> c87bd6ab95129e13a1821a27685a4c239d3c0556
        throws IOException, InterruptedException {
            for(Text v : value) {
                context.write(new Text(key.toString()), v);
        }
    }
}