package outputFormat;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * OutputFormat class for Invert Index.
 * 
 * @author RailgunHamster (王宇鑫 151220114)
 * @version 0.1
 * @date 2018/4/20
 */
public class InvertIndexOutputFormat extends TextOutputFormat<Text, Text> {
}