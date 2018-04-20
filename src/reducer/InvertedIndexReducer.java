package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public final static Class outputKeyClass = Text.class;
    public final static Class outputValueClass = IntWritable.class;

    private IntWritable result =  new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0;
        for (IntWritable val: values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}