package mission5.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitialMapper extends Mapper<Object, Text, Text, Text> {
  /*
  private Text name = new Text();
  private Text out = new Text();

  private static String mychangestr(String str) {
        String tempStr;
        int index1=str.indexOf('[');
        int index2=str.indexOf(',');
        tempStr = str.substring(index1 + 1, index2);
        String tempStr2=tempStr+':'+tempStr;
        String res=str.replace(tempStr,tempStr2);
        while(str.indexOf('|',index2+1)!=-1 && str.indexOf(',',index2+1)!=-1){
            String tempStr3;
            index1=str.indexOf('|',index2+1);
            index2=str.indexOf(',',index2+1);
            tempStr3 = str.substring(index1+1, index2);
            String tempStr4=tempStr3+':'+tempStr3;
            res=res.replace(tempStr3,tempStr4);
        }
        return res;
    }
    */
  @Override
  protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
      context.write(new Text("a"), new Text("b"));
      /*
      String str = value.toString();
      String[] tmp = str.split("\t");
      tmp[1]=mychangestr(tmp[1]);
      name.set(tmp[0]);
      out.set( tmp[0]+"\t" + tmp[1]);
      context.write(name, out);
      */
  }

}
