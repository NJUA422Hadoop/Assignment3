package mission4.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TheMapper extends Mapper<Object, Text, Text, Text> {
  @Override
  protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
      
      String line = value.toString();
      String[] tmp = line.split("\t");
      String name = tmp[0];
      double pr = Double.parseDouble(tmp[1]);

      String[] link = tmp[2].substring(1, tmp[2].length()-1).split("\\|");

      for(String l : link) {
          String linkPage = l.split(":")[0];
          String weight = l.split(":")[1];
          double prValue = pr * Double.parseDouble(weight);
          context.write(new Text(linkPage), new Text(String.valueOf(prValue)));
      }
      context.write(new Text(name), new Text("|" + tmp[2]));
    }
}