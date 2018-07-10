package mission5.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import tools.Tuple;

public class TheMapper extends Mapper<Object, Text, Text, Text> {
  private static final Text text1 = new Text();
  private static final Text text2 = new Text();
  private static final StringBuilder sb = new StringBuilder();
  private static Map<String, String> map = new HashMap<>();

  public static Tuple<String, String> findlabel(String str) {
    sb.delete(0, sb.length());
    double max=0;
    String res="";
    HashMap<String, Double> hm = new HashMap<>();
    String tempStr;
    String nameStr;
    int index1=str.indexOf('[');
    sb.append('[');
    int index2=str.indexOf('|');
    if(index2!=-1){
      tempStr = str.substring(index1 + 1, index2);
      String labelname1=labelname(tempStr);
      nameStr = name(tempStr);
      sb.append(nameStr);
      sb.append(',');
      sb.append(map.getOrDefault(nameStr, labelname1));
      double labelweight1=labelweight(tempStr);
      sb.append(':');
      sb.append(labelweight1);
      if(hm.containsKey(labelname1)){
        double newweight=hm.get(labelname1)+labelweight1;
        hm.put(labelname1,newweight);
      } else {
        hm.put(labelname1, labelweight1);
      }
      index1=str.indexOf('|',index2);
      index2=str.indexOf('|',index1+1);
      while(index1!=-1 &&index2!=-1){
        tempStr = str.substring(index1 + 1, index2);
        sb.append('|');
        nameStr = name(tempStr);
        sb.append(nameStr);
        String labelname2=labelname(tempStr);
        double labelweight2=labelweight(tempStr);
        sb.append(',');
        sb.append(map.getOrDefault(nameStr, labelname2));
        sb.append(':');
        sb.append(labelweight2);
        if(hm.containsKey(labelname2)){
          double newweight=hm.get(labelname2)+labelweight2;
          hm.put(labelname2,newweight);
        } else {
          hm.put(labelname2, labelweight2);
        }
        index1=str.indexOf('|',index2);
        index2=str.indexOf('|',index1+1);
      }
    }
    index2=str.indexOf(']');
    tempStr = str.substring(index1 + 1, index2);
    sb.append('|');
    nameStr = name(tempStr);
    sb.append(nameStr);
    String labelname3=labelname(tempStr);
    double labelweight3=labelweight(tempStr);
    sb.append(',');
    sb.append(map.getOrDefault(nameStr, labelname3));
    sb.append(':');
    sb.append(labelweight3);
    sb.append(']');
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
    return new Tuple<>(res, sb.toString());
  }

  private static  String name(String str){
    int index=str.indexOf(',');
    String tempStr = str.substring(0, index);
    return tempStr;
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
  protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    map.clear();
  }

  @Override
  protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
    String line = value.toString();
    String[] tmp = line.split("\t");
    String name = tmp[0];
    Tuple<String, String> tuple = new Tuple<>("", "");
    String label = "";
    if(tmp.length > 2) {
      tuple = findlabel(tmp[2]);
      label = map.getOrDefault(name, tuple.first);
      map.put(name, label);
    }
    text1.set(name);
    text2.set(label+"\t"+tuple.second);
    context.write(text1, text2);
  }
}
