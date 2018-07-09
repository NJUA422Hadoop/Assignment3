package mission5.reducer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TheReducer extends Reducer<Text, Text, Text, Text> {

  private Configuration conf;

  @Override
  protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    conf = context.getConfiguration();
  }

  private String replacestr(String str){
      String []res=str.split(",");
      res[1]=conf.get(res[0]);
      return res[0]+','+res[1];
  }
  @Override
  protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
      for(Text v : value) {
          String name=key.toString();
          String str = v.toString();
          String res="[";
          String tempStr;
          int index1=str.indexOf('[');
          int index2=str.indexOf('|');
          tempStr = str.substring(index1 + 1, index2);
          String []ts=tempStr.split(":");
          ts[0]=replacestr(ts[0]);
          res=res+ts[0]+":"+ts[1];
          //System.out.println(tempStr);
          while(str.indexOf('|',index2+1)!=-1 && str.indexOf('|',index2+1)!=-1){
            String tempStr2;
            index1=str.indexOf('|',index2);
            index2=str.indexOf('|',index1+1);
            tempStr2 = str.substring(index1+1, index2);
            String []ts2=tempStr2.split(":");
            ts2[0]=replacestr(ts2[0]);
            res=res+"|"+ts2[0]+":"+ts2[1];
            //System.out.println(tempStr2);
          }
          index1=index2;
          index2=str.indexOf(']');
          tempStr = str.substring(index1 + 1, index2);
          String []ts3=tempStr.split(":");
          ts3[0]=replacestr(ts3[0]);
          res=res+"|"+ts3[0]+":"+ts3[1]+"]";
          context.write(key, new Text(conf.get(key.toString())+'\t'+res));

  }
}
