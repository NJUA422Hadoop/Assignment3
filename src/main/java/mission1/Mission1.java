package mission1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;

import tools.BaseMission;;

/**
 * 第一个任务
 * 做这个任务的人写一下描述：
 *  XXX
 */

public class Mission1 extends BaseMission {
  public static final String input = "???";
  public static final String output = "???";

  public Mission1(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf() {
    conf.set("input", input); // or args[?]
    conf.set("output", output);
    // more settings
  }

  @Override
  protected void setupJob() {
    // set mapper class ... etc
    // file input path ...
  }
}