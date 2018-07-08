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

      if(tmp.length > 2) {
          String[] link = tmp[2].split("\\|");
          link[0] = link[0].substring(1);
          link[link.length-1] = link[link.length-1].substring(0,link[link.length-1].length()-1);
          for(String l:link) {
              String linkPage = l.split(",")[0];
              String weight = l.split(",")[1];
              double prValue = pr * Double.parseDouble(weight);
              context.write(new Text(linkPage), new Text(String.valueOf(prValue)));
          }
          context.write(new Text(name), new Text("|" + tmp[2]));
        }
      }
}