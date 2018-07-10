package mission7.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.FloatWritable;

public class TheMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
            String line = value.toString();
            String[] tmp = line.split("\t");
            context.write(new Text(tmp[1]), new Text(tmp[0] + "\t" + tmp[2]));
        }
}