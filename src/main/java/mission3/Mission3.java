package mission3;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;

import mission2.Mission2;
import mission3.mapper.TheMapper;
import mission3.reducer.TheReducer;
import tools.BaseMission;;

/**
 * 第三个任务
 * 做这个任务的人写一下描述：
 *  XXX
 */

public class Mission3 extends BaseMission {
  public static final String output = "mission3";

  public Mission3(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf() {
    conf.set("input", args[1] + "/" + Mission2.output + "/part*");
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

  @Override
  protected String getDependecies() {
    // 依赖任务二
    return "2";
  }

  @Override
  public boolean isWorking() {
    return true;
  }
}