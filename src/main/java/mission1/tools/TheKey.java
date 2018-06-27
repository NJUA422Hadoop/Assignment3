package mission1.tools;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import tools.Tuple;

/**
 * Tuple&lt;Text, LongWritable&gt;的简化。
 * <pre>
 * 来自
 * hadoop.io.Text
 * hadoop.io.LongWritable
 * </pre>
 * @author RailgunHasmter（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/28
 */
public class TheKey extends Tuple<Text, LongWritable> {}