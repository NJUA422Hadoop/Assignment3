import mapper.InvertedIndexMapper;
import reducer.InvertedIndexReducer;
import combiner.InvertedIndexCombiner;
import partitioner.InvertedIndexPartitioner;
import inputFormat.InvertedIndexInputFormat;
import outputFormat.InvertedIndexOutputFormat;
import outputFormat.HBaseInvertedIndexOutputFormat;
import hbase.HBase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

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
 * @version 1.1
 * @date 2018/4/19
 */
public class InvertedIndexAssignment extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String table = "Wuxia";

        HBase hbase = HBase.shared;
        hbase.createTable(table, InvertedIndexReducer.columnFamily);

        // config
        Configuration conf = getConf();
        hbase.set(conf);
        conf.set("input", args[0]);
        conf.set("output", args[1]);
        conf.set(HBaseInvertedIndexOutputFormat.OUTPUT_TABLE, table);

        // job
        Job job = Job.getInstance(conf, InvertedIndexAssignment.class.getSimpleName());
        job.setJarByClass(InvertedIndexAssignment.class);

        // mapper
        job.setMapperClass(InvertedIndexMapper.class);
        job.setMapOutputKeyClass(InvertedIndexMapper.outputKeyClass);
        job.setMapOutputValueClass(InvertedIndexMapper.outputValueClass);

        // reducer
        TableMapReduceUtil.initTableReducerJob(table, InvertedIndexReducer.class, job);
        job.setOutputKeyClass(InvertedIndexReducer.outputKeyClass);
        job.setOutputValueClass(InvertedIndexReducer.outputValueClass);
        MultipleOutputs.addNamedOutput(
            job, 
            "hdfs",
            InvertedIndexOutputFormat.class,
            InvertedIndexOutputFormat.outputKeyClass,
            InvertedIndexOutputFormat.outputValueClass
        );

        // combiner
        job.setCombinerClass(InvertedIndexCombiner.class);

        // partitioner
        job.setPartitionerClass(InvertedIndexPartitioner.class);

        // input/output format
        job.setInputFormatClass(InvertedIndexInputFormat.class);
        job.setOutputFormatClass(HBaseInvertedIndexOutputFormat.class);

        // add input/output path
        InvertedIndexInputFormat.addInputPath(job, new Path(conf.get("input")));

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
