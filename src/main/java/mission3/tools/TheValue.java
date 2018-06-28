package mission3.tools;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import tools.Tuple;

/**
 * {@link Tuple}&lt;{@link Text}, {@link IntWritable}&gt;的简化。
 * @author RailgunHasmter（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/28
 */
public class TheValue extends Tuple<Text, IntWritable> {
  public TheValue(Text first, IntWritable second) {
    super(first, second);
  }

  public TheValue() {
    ;
  }
}