package mission1.tools;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import tools.Tuple;

/**
 * {@link Tuple}&lt;{@link Text}, {@link LongWritable}&gt;的简化。
 * @author RailgunHasmter（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/28
 */
public class TheKey extends Tuple<Text, LongWritable> {
  public TheKey(Text first, LongWritable second) {
    super(first, second);
  }

  public TheKey() {
    ;
  }
}