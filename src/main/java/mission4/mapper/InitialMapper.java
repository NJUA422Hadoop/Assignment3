package mission4.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitialMapper extends Mapper<Object, Text, Text, Text> {
  private Text name = new Text();
  private Text out = new Text();

  @Override
  protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
      String str = value.toString();
      String[] tmp = str.split("\t");
      name.set(tmp[0]);
      out.set("1" + "\t" + tmp[1]);
      context.write(name, out);
  }
}