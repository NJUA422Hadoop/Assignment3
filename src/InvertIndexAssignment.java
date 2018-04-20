import mapper;
import reducer;
import partitioner;
import inputFormat;
import outputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;

/**
 * InvertIndexAssignment
 * 依赖实现：
 * Mapper:
 * 1. class name: mapper.InvertIndexMapper
 * 2. public static Class<? extends Writable> outputKeyClass = <output Key class>;
 * 3. public static Class<? extends Writable> outputValueClass = <output Value class>;
 * Reducer:
 * 1. class name: reducer.InvertIndexReducer
 * 2. public static Class<? extends Writable> outputKeyClass = <output Key class>;
 * 3. public static Class<? extends Writable> outputValueClass = <output Value class>;
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
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.addOutputPath(job, new Path(conf.get("output")));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(new InvertIndexAssignment(), args));
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}