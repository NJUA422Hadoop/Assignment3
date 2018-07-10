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

public class TheReducer extends Reducer<Text, Text, Text, Text> {
  private Text text = new Text();
  private Map<String, String> map = new HashMap<>();

  @Override
  protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    FileSystem fs = FileSystem.get(conf);

    FSDataInputStream in = fs.open(new Path(conf.get("output") + "/labels.txt"));
    LineReader lineReader = new LineReader(in);

    int length = -1;
    while (length != 0) {
      length = lineReader.readLine(text);
      String[] line = text.toString().trim().split("\t");
      map.put(line[0], line[1]);
    }

    lineReader.close();
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
