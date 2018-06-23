package mission2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;

import mission1.Mission1;
import mission2.mapper.TheMapper;
import mission2.reducer.TheReducer;
import tools.BaseMission;;

/**
 * 第二个任务
 * 做这个任务的人写一下描述：
 *  XXX
 */

public class Mission2 extends BaseMission {
  public static final String output = "mission2";

  public Mission2(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf() {
    conf.set("input", args[0] + "/" + Mission1.output);
    conf.set("output", args[1] + "/" + output);
    // more settings
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

  @Override
  protected String getDependecies() {
    // 依赖任务一
    return "1";
  }
}