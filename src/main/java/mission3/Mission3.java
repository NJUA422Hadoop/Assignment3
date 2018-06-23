package mission3;

import org.apache.hadoop.conf.Configured;

import tools.BaseMission;;

/**
 * 第三个任务
 * 做这个任务的人写一下描述：
 *  XXX
 */

public class Mission3 extends BaseMission {
  public static final String input = "???";
  public static final String output = "???";

  public Mission3(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf() {
    conf.set("input", input); // or args[?] or Mission2.output
    conf.set("output", output);
    // more settings
  }

  @Override
  protected void setupJob() {
    // set mapper class ... etc
  }

  @Override
  protected String getDependecies() {
    // 依赖任务二
    return "2";
  }
}