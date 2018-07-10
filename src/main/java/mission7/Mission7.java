package mission7;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import mission5.Mission5;
import mission7.mapper.TheMapper;

import tools.BaseMission;

/**
 * 第六个任务
 * 分析结果整理
    （1）PageRank值进行排序
    （2）将属于同一个标签的人物输出到一起
 *  wyd
 */

public class Mission7 extends BaseMission {
  public static final String input = "mission5";
  public static final String output = "mission7";

  public Mission7(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf(int index) {
    conf.set("input", args[1] + "/" + Mission5.output + "/" + Mission5.resultPath);
    conf.set("output", args[1] + "/" + output);
  }

  @Override
  protected Job setupJob(Job job, int index) {
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(TheMapper.class);
    return job;
  }

  @Override
  protected String getDependecies() {
    // 依赖任务五
    return "5";
  }

  @Override
  public boolean isWorking() {
    return true;
  }
}
