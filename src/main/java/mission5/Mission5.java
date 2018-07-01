package mission5;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;

import tools.BaseMission;

/**
 * 第五个任务
 * 做这个任务的人写一下描述：
 *  XXX
 */

public class Mission5 extends BaseMission {
  public static final String input = "???";
  public static final String output = "???";

  public Mission5(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf(int index) {
    conf.set("input", input); // or args[?] or Mission3.output
    conf.set("output", output);
    // more settings
  }

  @Override
  protected Job setupJob(Job job, int index) {
    // set mapper class ... etc
    return job;
  }

  @Override
  protected String getDependecies() {
    // 依赖任务三
    return "3";
  }

  @Override
  public boolean isWorking() {
    return false;
  }
}