package mission2.tools;

import org.apache.hadoop.io.Text;

import tools.Tuple;

/**
 * {@link Tuple}&lt;{@link Text}, {@link Text}&gt;的简化。
 * @author RailgunHasmter（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/28
 */
public class TheKey extends Tuple<Text, Text> {
  public TheKey(Text first, Text second) {
    super(first, second);
  }

  public TheKey() {
    ;
  }
}