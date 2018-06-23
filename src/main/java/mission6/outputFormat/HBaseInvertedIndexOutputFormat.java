package mission6.outputFormat;

import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * HBase OutputFormat class for Invert Index.
 * 
 * @author RailgunHamster (王宇鑫 151220114)
 * @version 1.1
 * @date 2018/5/15
 */
public class HBaseInvertedIndexOutputFormat extends TableOutputFormat<ImmutableBytesWritable> {
}