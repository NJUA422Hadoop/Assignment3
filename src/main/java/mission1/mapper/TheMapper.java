package mission1.mapper;

import java.io.IOException;
import java.util.List;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TheMapper extends Mapper<Object, Text, NullWritable, Text> {
  @Override
  protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context)
      throws IOException, InterruptedException {
    AnsjLoader.shared.ENVload();
  }

  @Override
  protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context)
      throws IOException, InterruptedException {
    Result result = ToAnalysis.parse(value.toString());

    List<Term> words = result.getTerms();

    for (Term t : words) {
      context.write(NullWritable.get(), new Text(t.getRealName()));
    }
  }
}