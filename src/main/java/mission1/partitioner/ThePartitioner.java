package mission1.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import mission1.tools.TheKey;

public class ThePartitioner extends Partitioner<TheKey, Text> {
  private static final Text text = new Text();
  private static Partitioner<Text, Text> partitioner = new HashPartitioner<>();

  @Override
  public int getPartition(TheKey key, Text value, int numPartitions) {
    text.set(key.first);
    return partitioner.getPartition(text, value, numPartitions);
  }
}