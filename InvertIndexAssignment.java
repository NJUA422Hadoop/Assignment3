import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;

/**
 * InvertIndexAssignment
 * 依赖：
 *  Mapper:
 *  1. class name : InvertIndexMapper
 *  2. public static Class<? extends Writable> outputKeyClass = <output Key class>;
 *  3. public static Class<? extends Writable> outputValueClass = <output Value class>;
 *  Reducer:
 *  1. class name : InvertIndexReducer
 *  2. public static Class<? extends Writable> outputKeyClass = <output Key class>;
 *  3. public static Class<? extends Writable> outputValueClass = <output Value class>;
 *  Partitioner:
 *  1. class name: InvertIndexPartitioner
 *  InputFormat:
 *  1. class name: InvertIndexInputFormat
 *  OutputFormat:
 *  1. class name: InvertIndexOutputFormat
 */
public class InvertIndexAssignment extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // config
        Configuration conf = getConf();

        // job
        Job job = Job.getInstance(conf, InvertIndexAssignment.class.getSimpleName());
        job.setJarByClass(InvertIndexAssignment.class);

        // mapper
        job.setMapperClass(InvertIndexMapper.class);
        job.setMapOutputKeyClass(InvertIndexMapper.outputKeyClass);
        job.setMapOutputValueClass(InvertIndexMapper.outputValueClass);

        // reducer
        job.setReducerClass(InvertIndexReducer.class);
        job.setOutputKeyClass(InvertIndexReducer.outputKeyClass);
        job.setOutputValueClass(InvertIndexReducer.outputValueClass);

        // partitioner
        job.setPartitionerClass(InvertIndexPartitioner.class);

        // input/output format
        job.setInputFormatClass(InvertIndexInputFormat.class);
        job.setOutputFormatClass(InvertIndexOutputFormat.class);

        // add input/output path
        InvertIndexInputFormat.addInputPath(job, new Path(conf.get("input")));
        InvertIndexOutputFormat.addOutputPath(job, new Path(conf.get("output")));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new InvertIndexAssignment(), args));
    }
}