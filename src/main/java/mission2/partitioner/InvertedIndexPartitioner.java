package mission2.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Partitioner class for Invert Index.
 * 
 * @author RailgunHamster (王宇鑫 151220114)
 * @version 0.1
 * @date 2018/4/20
 */
public class InvertedIndexPartitioner extends HashPartitioner<Text, IntWritable> {
    public static final String intervals = "[@#]";

    public Text getWord(Text key) {
        return new Text(key.toString().split(intervals)[0]);
    }

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        return super.getPartition(
            getWord(key),
            value,
            numReduceTasks
        );
    }
}