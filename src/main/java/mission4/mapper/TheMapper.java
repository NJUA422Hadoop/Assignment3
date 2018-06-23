package mission4.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TheMapper extends Mapper<Object, Text, Text, Text> {
  @Override
  protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
    context.write(new Text("mission4"), value);
  }
}