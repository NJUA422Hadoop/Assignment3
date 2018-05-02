package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 输入: key为Object类型, 代表行偏移量, value为Text类型, 代表行内容
 * 输出: key为Text类型, 格式为word@fileName, value为IntWritable类型, 代表词在文中出现一次
 * @author WaterYe(王尧 151220112)
 */

public class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
    public final static Class<? extends Writable> outputKeyClass = Text.class;
    public final static Class<? extends Writable> outputValueClass = IntWritable.class;

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName().replaceAll(".txt.segmented", "");
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()) {
            word.set(itr.nextToken() + "@" + fileName);
            context.write(word, one);
        }
    }
}