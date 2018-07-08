package mission4;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import mission3.Mission3;
import mission4.mapper.TheMapper;
import mission4.mapper.InitialMapper;
import mission4.reducer.TheReducer;
import mission4.reducer.InitialReducer;
import tools.BaseMission;

/**
 * 第四个任务:
 * <pre>
 * 基于人物关系图的PageRank计算
 * 输入: Mission3的输出，格式如下
 * 狄云   [戚芳:0.33333|戚长发:0.333333|卜垣:0.333333]
 * 戚芳   [狄云:0.25|戚长发:0.25|卜垣:0.5]
 * 戚长发 [狄云:0.33333|戚芳:0.333333|卜垣:0.333333]
 * 卜垣   [狄云:0.25|戚芳:0.5|戚长发:0.25]
 * 输出: 人物的PageRank值
 * </pre>
 * @author WaterYe（王尧 151220112）
 * @version 1.0
 * @date 2018/7/4
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
    if (index == 1) {
      job.setMapperClass(InitialMapper.class);
      job.setReducerClass(InitialReducer.class);
  
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setInputFormatClass(KeyValueTextInputFormat.class);
    }
    else {
      job.setMapperClass(TheMapper.class);
      job.setReducerClass(TheReducer.class);
  
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setInputFormatClass(KeyValueTextInputFormat.class);
    }
    
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
    return true;
  }
}