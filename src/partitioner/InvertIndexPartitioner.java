package partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;

/**
 * Partitioner class for Invert Index.
 * 
 * @author RailgunHamster (王宇鑫 151220114)
 * @version 0.1
 * @date 2018/4/20
 */
public class InvertIndexPartitioner extends HashPartitioner<Text, IntWritable> {
}