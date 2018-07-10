package mission5.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.LineReader;

import mission5.mapper.DeprecatedMapper;

public class DeprecatedReducer extends Reducer<Text, Text, Text, Text> {
  private Text text = new Text();
  private Map<String, String> map = new HashMap<>();

  @Override
  protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    FileSystem fs = FileSystem.newInstance(conf);

    Path path = new Path(conf.get("output") + DeprecatedMapper.tempPath);

    FSDataInputStream in = fs.open(path);

    byte[] buffer = new byte[in.available()];

    in.read(buffer);
    Text t = new Text(buffer);

    String[] tuples = t.toString().split(DeprecatedMapper.newline);
    
    for (String tuple : tuples) {
      String[] split = tuple.split(DeprecatedMapper.delimeter);
      String name = split[0];
      String label = split[1];
      map.put(name, label);
    }

    in.close();
    fs.close();
  }

  private String replacestr(String str){
      String []res=str.split(",");
      res[1]=map.get(res[0]);
      return res[0]+','+res[1];
  }

  @Override
  protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
    for(Text v : value) {
      String str = v.toString();
      String res="[";
      String tempStr;
      int index1=str.indexOf('[');
      int index2=str.indexOf('|');
      if(index2!=-1){
        tempStr = str.substring(index1 + 1, index2);
        String []ts=tempStr.split(":");
        ts[0]=replacestr(ts[0]);
        res=res+ts[0]+":"+ts[1];
        while(str.indexOf('|',index2+1)!=-1 && str.indexOf('|',index2+1)!=-1){
          String tempStr2;
          index1=str.indexOf('|',index2);
          index2=str.indexOf('|',index1+1);
          tempStr2 = str.substring(index1+1, index2);
          String []ts2=tempStr2.split(":");
          ts2[0]=replacestr(ts2[0]);
          res=res+"|"+ts2[0]+":"+ts2[1];
        }
        index1=index2;
      }
      index2=str.indexOf(']');
      tempStr = str.substring(index1 + 1, index2);
      String []ts3=tempStr.split(":");
      ts3[0]=replacestr(ts3[0]);
      res=res+"|"+ts3[0]+":"+ts3[1]+"]";
      context.write(key, new Text(map.get(key.toString())+'\t'+res));
    }
  }
}
