package mission4;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import mission3.Mission3;
import mission4.mapper.TheMapper;
import mission4.reducer.TheReducer;
import tools.BaseMission;

/**
 * 第四个任务
 * 做这个任务的人写一下描述：
 *  XXX
 */

public class Mission4 extends BaseMission {
  public static final String output = "mission4";

  public Mission4(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf(int index) {
    if (index == 1) {
      conf.set("input", args[1] + "/" + Mission3.output);
      conf.set("output", args[1] + "/" + output + "/" + index);
    } else {
      conf.set("input", args[1] + "/" + output + "/" + (index - 1));
      conf.set("output", args[1] + "/" + output + "/" + index);
    }
  }

  @Override
  protected Job setupJob(Job job, int index) {
    job.setMapperClass(TheMapper.class);
    job.setReducerClass(TheReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    return job;
  }

  @Override
  protected String getDependecies() {
    // 依赖任务三
    return "3";
  }

  @Override
  protected int times() {
    return 2;
  }

  @Override
  public boolean isWorking() {
    return false;
  }
}