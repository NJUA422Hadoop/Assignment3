package mission5;

import org.apache.hadoop.conf.Configured;

import tools.BaseMission;;

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
  protected void setupConf() {
    conf.set("input", input); // or args[?] or Mission3.output
    conf.set("output", output);
    // more settings
  }

  @Override
  protected void setupJob() {
    // set mapper class ... etc
  }

  @Override
  public boolean isWorking() {
    return false;
  }

  @Override
  protected String getDependecies() {
    // 依赖任务三
    return "3";
  }
}