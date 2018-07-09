package mission5;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import mission3.Mission3;

import tools.BaseMission;

 /**
 * 第五个任务
 * <pre>
 * 在任务关系图上的标签传播
 * 输入: Mission3的输出，格式如下
 * 狄云   [戚芳:0.33333|戚长发:0.333333|卜垣:0.333333]
 * 戚芳   [狄云:0.25|戚长发:0.25|卜垣:0.5]
 * 戚长发 [狄云:0.33333|戚芳:0.333333|卜垣:0.333333]
 * 卜垣   [狄云:0.25|戚芳:0.5|戚长发:0.25]
 * 输出: 任务的标签信息
 * </pre>
 * @author WaterYe（王尧 151220112）
 * @version 1.0
 * @date 2018/7/7
 */

public class Mission5 extends BaseMission {
  public static final String output = "mission5";

  public Mission5(Configured self, String[] args) {
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
    // more settings
  }

  @Override
  protected Job setupJob(Job job, int index) {
    // set mapper class ... etc
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    return job;
  }

  @Override
  protected String getDependecies() {
    // 依赖任务三
    return "3";
  }

  @Override
  public boolean isWorking() {
    return true;
  }
}
