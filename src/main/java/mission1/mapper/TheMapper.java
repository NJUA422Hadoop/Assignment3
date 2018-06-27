package mission1.mapper;

import java.io.IOException;
import java.util.List;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TheMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
  private List<String> names;

  @Override
  protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
      throws IOException, InterruptedException {
    AnsjLoader.shared.ENVload();

    names = AnsjLoader.shared.getSource("wuxia_name.txt");
  }

  @Override
  protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
      throws IOException, InterruptedException {
    Result result = ToAnalysis.parse(value.toString());

    List<Term> words = result.getTerms();

    for (Term t : words) {
      String name = t.getRealName();

      if (names.contains(name)) {
        context.write(key, new Text(name));
      }
    }
  }
}