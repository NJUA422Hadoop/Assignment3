package mission6.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankSortMapper extends Mapper<Object, Text, Text, Text> {
    private Text page = new Text();
    private FloatWritable pr = new FloatWritable();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
            String line = value.toString();
            String[] tmp = line.split("\t");
            page.set(tmp[0]);
            pr.set(Float.parseFloat(tmp[1]));
            context.write(pr, page);
        }
    
    public static class FloatComparator extends FloatWritable.Comparator {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
}