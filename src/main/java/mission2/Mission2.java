package mission2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import tools.BaseMission;;

/**
 * 第二个任务
 * 做这个任务的人写一下描述：
 *  XXX
 */

public class Mission2 extends BaseMission {
  public static final String input = "???";
  public static final String output = "???";

  public Mission2(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf() {
    conf.set("input", input); // or args[?] or Mission1.output
    conf.set("output", output);
    // more settings
  }

  @Override
  protected void setupJob() {
    // set mapper class ... etc
  }

  @Override
  protected String getDependecies() {
    // 依赖任务一
    return "1";
  }
}