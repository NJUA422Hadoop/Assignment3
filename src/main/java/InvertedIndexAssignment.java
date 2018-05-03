import mapper.InvertedIndexMapper;
import reducer.InvertedIndexReducer;
import combiner.InvertedIndexCombiner;
import partitioner.InvertedIndexPartitioner;
import inputFormat.InvertedIndexInputFormat;
import outputFormat.InvertedIndexOutputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;

/**
 * InvertedIndexAssignment
 * 依赖实现：
 * Mapper:
 * 1. class name: mapper.InvertIndexMapper
 * 2. public final static Class outputKeyClass = <output Key class>;
 * 3. public fianl static Class outputValueClass = <output Value class>;
 * Reducer:
 * 1. class name: reducer.InvertIndexReducer
 * 2. public final static Class outputKeyClass = <output Key class>;
 * 3. public fianl static Class outputValueClass = <output Value class>;
 * Partitioner:
 * 1. class name: partitioner.InvertIndexPartitioner
 * InputFormat:
 * 1. class name: inputFormat.InvertIndexInputFormat
 * OutputFormat:
 * 1. class name: outputFormat.InvertIndexOutputFormat
 * @author RailgunHamster (王宇鑫 151220114)
 * @version 0.1
 * @date 2018/4/19
 */
public class InvertedIndexAssignment extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // config
        Configuration conf = getConf();
        conf.set("input", args[0]);
        conf.set("output", args[1]);

        // job
        Job job = Job.getInstance(conf, InvertedIndexAssignment.class.getSimpleName());
        job.setJarByClass(InvertedIndexAssignment.class);

        // mapper
        job.setMapperClass(InvertedIndexMapper.class);
        job.setMapOutputKeyClass(InvertedIndexMapper.outputKeyClass);
        job.setMapOutputValueClass(InvertedIndexMapper.outputValueClass);

        // reducer
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(InvertedIndexReducer.outputKeyClass);
        job.setOutputValueClass(InvertedIndexReducer.outputValueClass);

        // combiner
        job.setCombinerClass(InvertedIndexCombiner.class);

        // partitioner
        job.setPartitionerClass(InvertedIndexPartitioner.class);

        // input/output format
        job.setInputFormatClass(InvertedIndexInputFormat.class);
        job.setOutputFormatClass(InvertedIndexOutputFormat.class);

        // add input/output path
        InvertedIndexInputFormat.addInputPath(job, new Path(conf.get("input")));
        InvertedIndexOutputFormat.setOutputPath(job, new Path(conf.get("output")));

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