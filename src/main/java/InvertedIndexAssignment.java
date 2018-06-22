import mapper.InvertedIndexMapper;
import reducer.InvertedIndexReducer;
import combiner.InvertedIndexCombiner;
import partitioner.InvertedIndexPartitioner;
import inputFormat.InvertedIndexInputFormat;
import outputFormat.InvertedIndexOutputFormat;
import outputFormat.HBaseInvertedIndexOutputFormat;
import hbase.HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;

/**
 * InvertedIndexAssignment
 * @author RailgunHamster (王宇鑫 151220114)
 * @version 2.0
 * @date 2018/6/22
 */
public class InvertedIndexAssignment extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // config
        Configuration conf = getConf();
        conf.set("input", args[0]);
        conf.set("output", args[1]);

        // job
        Class<?> self = InvertedIndexAssignment.class;
        Job job = Job.getInstance(conf, self.getSimpleName());
        job.setJarByClass(self);

        // jobs

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(new InvertedIndexAssignment(), args));
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
