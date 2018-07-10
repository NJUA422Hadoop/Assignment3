package mission1.mapper;

import java.io.IOException;
import java.util.List;

import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mission1.tools.TheKey;
import tools.Loader;

public class TheMapper extends Mapper<TheKey, Text, TheKey, Text> {
  private final Text text = new Text();
  private List<String> names;

  @Override
  protected void setup(Mapper<TheKey, Text, TheKey, Text>.Context context)
      throws IOException, InterruptedException {
    final Loader loader = new Loader(TheMapper.class);

    for (String name : loader.getSource("wuxia_name.txt")) {
      DicLibrary.insert(DicLibrary.DEFAULT, name);
    }

    names = loader.getSource("wuxia_name.txt");
  }

  @Override
  protected void map(TheKey key, Text value, Mapper<TheKey, Text, TheKey, Text>.Context context)
      throws IOException, InterruptedException {
    List<Term> words = ToAnalysis.parse(value.toString().trim()).getTerms();

    for (Term t : words) {
      String name = t.getRealName();

      if (names.contains(name)) {
        text.set(name);
        context.write(key, text);
      }
    }
  }
}