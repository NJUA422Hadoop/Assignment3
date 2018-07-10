package mission5.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TheMapper extends Mapper<Object, Text, Text, Text> {
  private Map<String, String> map = new HashMap<>();

  public static String findlabel(String str) {
    double max=0;
    String res="";
    HashMap<String, Double> hm = new HashMap<>();
    String tempStr;
    int index1=str.indexOf('[');
    int index2=str.indexOf('|');
    if(index2!=-1){
      tempStr = str.substring(index1 + 1, index2);
      String labelname1=labelname(tempStr);
      double labelweight1=labelweight(tempStr);
      if(hm.containsKey(labelname1)){
        double newweight=hm.get(labelname1)+labelweight1;
        hm.put(labelname1,newweight);
      }
      else {
        hm.put(labelname1, labelweight1);
      }
      index1=str.indexOf('|',index2);
      index2=str.indexOf('|',index1+1);
      while(index1!=-1 &&index2!=-1){
        tempStr = str.substring(index1 + 1, index2);
        String labelname2=labelname(tempStr);
        double labelweight2=labelweight(tempStr);
        if(hm.containsKey(labelname2)){
          double newweight=hm.get(labelname2)+labelweight2;
          hm.put(labelname2,newweight);
        }
        else {
          hm.put(labelname2, labelweight2);
        }
        index1=str.indexOf('|',index2);
        index2=str.indexOf('|',index1+1);
      }
    }
    index2=str.indexOf(']');
    tempStr = str.substring(index1 + 1, index2);
    String labelname3=labelname(tempStr);
    double labelweight3=labelweight(tempStr);
    if(hm.containsKey(labelname3)){
      double newweight=hm.get(labelname3)+labelweight3;
      hm.put(labelname3,newweight);
    }
    else {
      hm.put(labelname3, labelweight3);
    }
    for (String key : hm.keySet()) {
      if(hm.get(key)>max){
        max=hm.get(key);
        res=key;
      }
    }
    return res;
  }

  private static  String labelname(String str){
    int index1=str.indexOf(',');
    int index2=str.indexOf(':');
    String tempStr = str.substring(index1 + 1, index2);
    return tempStr;
  }

  private static  double labelweight(String str){
    int index1=str.indexOf(':');
    int index2=str.length();
    String tempStr;
    tempStr = str.substring(index1 + 1, index2);
    double res=Double.parseDouble(tempStr);
    return res;
  }

  @Override
  protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
    String line = value.toString();
    String[] tmp = line.split("\t");
    String name = tmp[0];
    if(tmp.length > 2) {
      map.put(name, findlabel(tmp[2]));
    }
    context.write(new Text(name), new Text(tmp[1]+"\t"+tmp[2]));
  }

  private static final String utf8 = "UTF-8";
  public static final String delimeter = "\t";
  public static final String newline = "\n";
  private static byte[] _delimeter;
  private static byte[] _newline;
  public static final String tempPath = "/_temp/labels.txt";

  @Override
  protected void cleanup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    if (_delimeter == null) {
      _delimeter = delimeter.getBytes(utf8);
    }
    if (_newline == null) {
      _newline = newline.getBytes(utf8);
    }
    
    Configuration conf = context.getConfiguration();

    FileSystem fs = FileSystem.newInstance(conf);
    
    FSDataOutputStream out = fs.create(new Path(conf.get("output") + tempPath));

    for (Map.Entry<String, String> t : map.entrySet()) {
      out.write(t.getKey().getBytes(utf8));
      out.write(_delimeter);
      out.write(t.getValue().getBytes(utf8));
      out.write(_newline);
    }

    out.close();
    fs.close();
  }
}
