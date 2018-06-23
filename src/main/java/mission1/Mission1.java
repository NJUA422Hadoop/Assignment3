package mission1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;

import mission1.mapper.TheMapper;
import mission1.reducer.TheReducer;
import tools.BaseMission;;

/**
 * 第一个任务：
 * 对金庸小说数据集，根据人名进行分词。保存分词结果。
 */

public class Mission1 extends BaseMission {
  public static final String output = "mission1";

  public Mission1(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf() {
    conf.set("input", args[0]);
    conf.set("output", args[1] + "/" + output);
  }

  @Override
  protected void setupJob() {
    job.setMapperClass(TheMapper.class);
    job.setReducerClass(TheReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
  }
}