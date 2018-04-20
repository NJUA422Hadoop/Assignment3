package partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner class for Invert Index.
 * 
 * @author RailgunHamster (王宇鑫 151220114)
 * @version 0.1
 * @date 2018/4/20
 */
public class InvertedIndexPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}