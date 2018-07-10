package mission5.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TheReducer extends Reducer<Text, Text, Text, Text> {
  private static final Text text = new Text();

<<<<<<< HEAD
  private Configuration conf;

  @Override
  protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    conf = context.getConfiguration();
  }

  private String replacestr(String str){
      String []res=str.split(",");
      //res[1]=conf.get(res[0]);
      return res[0]+','+res[1];
  }
=======
>>>>>>> efe74cfe282304e7a7ecc524baf2deca3f1bf7d4
  @Override
  protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
    for(Text v : value) {
      text.set(v);
      context.write(key, text);
    }
  }
}
