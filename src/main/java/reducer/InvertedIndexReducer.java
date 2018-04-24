package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
  public final static Class outputKeyClass  =Text.class;
  public final static Class outputValueClass = Text.class;
  private String term = new String();//临时存储word#filename中的word
  private String last = " ";//临时存储上一个word
  private int countItem;//统计word出现次数
  private int countDoc;//统计word出现文件数
  private StringBuilder out = new StringBuilder();//临时存储输出的value部分
  private float f;//临时计算平均出现频率
  public static final String intervals = "[@#]";
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      term = key.toString().split(intervals)[0];//获取word
      if (!term.equals(last)) {//此次word与上次不一样，则将上次进行处理并输出
          if (!last.equals(" ")) {//避免第一次比较时出错
              out.setLength(out.length() - 1);//删除value部分最后的;符号
              f = (float) countItem / countDoc;//计算平均出现次数
              context.write(new Text(last), new Text(String.format("%.2f,%s", f, out.toString())));//value部分拼接后输出
              countItem = 0;//以下清除变量，初始化计算下一个word
              countDoc = 0;
              out = new StringBuilder();
          }
          last = term;//更新word，为下一次做准备
      }
      int sum = 0;//累加相同word和filename中出现次数
      for (IntWritable val : values) {
          sum += val.get();
      }
      out.append(key.toString().split(intervals)[1] + ":" + sum + ";");//将filename:NUM; 临时存储
      countItem += sum;
      countDoc += 1;
  }
  public void cleanup(Context context) throws IOException, InterruptedException {
      out.setLength(out.length() - 1);
      f = (float) countItem / countDoc;
      context.write(new Text(last), new Text(String.format("%.2f,%s", f, out.toString())));
  }
}
